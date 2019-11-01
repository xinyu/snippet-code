#include <vector>
#include <map>
#include <time.h>
#include "WriteCassandra.h"


void WriteCassandra::print_error(CassFuture* future) 
{
    const char* message;
    size_t message_length;
    cass_future_error_message(future, &message, &message_length);
    MYLOG_ERROR(g_pLogger, "Cassandra Error: %s", message);
}


bool WriteCassandra::init()
{   
    if (!create_cluster())
    {
        return false;
    }

    if (CASS_OK != connect_session()) 
    {
        cass_cluster_free(_cluster);
        cass_session_free(_session);
        return false;
    }

    return true;
}

void WriteCassandra::close()
{
    _close_future = cass_session_close(_session);
    cass_future_wait(_close_future);
    cass_future_free(_close_future);

    cass_cluster_free(_cluster);
    cass_session_free(_session);
}

bool WriteCassandra::create_cluster()
{
    _cluster = cass_cluster_new();
    cass_cluster_set_contact_points(_cluster, g_cassandra_hosts.c_str());

    return true;
}

CassError WriteCassandra::connect_session() 
{
  CassError rc = CASS_OK;
  CassFuture* future = cass_session_connect(_session, _cluster);

  cass_future_wait(future);
  rc = cass_future_error_code(future);
  if (rc != CASS_OK) 
  {
      print_error(future);
  }

  cass_future_free(future);

  return rc;
}

CassError WriteCassandra::execute_query(const char* query) 
{
  CassError rc = CASS_OK;
  CassFuture* future = NULL;
  CassStatement* statement = cass_statement_new(query, 0);

  future = cass_session_execute(_session, statement);
  cass_future_wait(future);

  rc = cass_future_error_code(future);
  if (rc != CASS_OK) 
  {
      print_error(future);
  }

  cass_future_free(future);
  cass_statement_free(statement);

  return rc;
}

CassError WriteCassandra::insert_log_batch(std::list<std::string>& values)
{
    CassError rc = CASS_OK;
    rc = insert_nginxlog_by_id(values);
    rc = insert_nginxlog_by_day(values);

    return rc;
}

CassError WriteCassandra::insert_nginxlog_by_id(std::list<std::string>& values)
{
    CassError rc = CASS_OK;

    struct timeval t1,t2;
    gettimeofday(&t1, NULL);

    std::string query_str = "INSERT INTO " + g_cassandra_table \
       + " (logdate, log_type, log_id, create_time, raw_data) \
          VALUES (?, ?, ?, ?, ?);" ;

    const char* query = query_str.c_str();
    int key_size = 5;

    int batch_size = 200;
    int vector_size = static_cast<int>(values.size());
    std::list<std::string>::iterator it;

    it = values.begin();

    for(int i = 0; i < vector_size; i += batch_size)
    {
        int batch_size_current = min(i + batch_size, vector_size);
        MYLOG_DEBUG(g_pLogger, "batch insert, from: %d, to: %d", i, batch_size_current);

        CassBatch* batch = cass_batch_new(CASS_BATCH_TYPE_LOGGED);

        for (int j = i; j < batch_size_current; j++)
        {
            const string& strRecord = *it;

            std::vector<std::string> elems;
            split(strRecord, '|', elems);
            if (elems.size() != 3)
            {
                MYLOG_WARN(g_pLogger, "Record Parse Fail, str=%s", strRecord.c_str());
                continue;
            }

            string log_type = elems[0];
            string log_time = elems[1];
            string raw_data = elems[2];

            MYLOG_DEBUG(g_pLogger, "log_type: %s, log_time:%s, raw_data: %s.", 
              log_type.c_str(), log_time.c_str(), raw_data.c_str());

            time_t timestamp_t;
            StringToTimeEX(log_time, timestamp_t);
            cass_int64_t timestamp_int64_t = (cass_int64_t)(timestamp_t * 1000);

            CassUuidGen* uuid_gen = cass_uuid_gen_new();
            CassUuid uuid;
            cass_uuid_gen_from_time(uuid_gen, timestamp_int64_t, &uuid);

            string logdate;
            TimeToStringEX(logdate, timestamp_t, 3);

            CassStatement* statement = cass_statement_new(query, key_size);

            cass_statement_bind_string(statement, 0, logdate.c_str());
            cass_statement_bind_string(statement, 1, log_type.c_str());
            cass_statement_bind_uuid(statement,   2, uuid);
            cass_statement_bind_int64(statement,  3, timestamp_int64_t);
            cass_statement_bind_string(statement, 4, raw_data.c_str());

            cass_batch_add_statement(batch, statement);
            cass_uuid_gen_free(uuid_gen);
            cass_statement_free(statement);

            std::advance(it, 1);
        }

        CassFuture* batch_future = cass_session_execute_batch(_session, batch);
        cass_batch_free(batch);

        rc = cass_future_error_code(batch_future);
        if (rc != CASS_OK) 
        {
            print_error(batch_future);
        }

        cass_future_free(batch_future);
    }

    if(vector_size > batch_size)
    {
        gettimeofday(&t2, NULL);
        double t1_ms = (t1.tv_sec) * 1000 + (t1.tv_usec) / 1000 ;
        double t2_ms = (t2.tv_sec) * 1000 + (t2.tv_usec) / 1000 ; 
        double timespan = t2_ms - t1_ms;
        MYLOG_WARN(g_pLogger, "Write Cassandra Record size=%d cost[%0.2lf]ms", vector_size, timespan);
    }

    return rc;
}

CassError WriteCassandra::insert_nginxlog_by_day(std::list<std::string>& values)
{
    CassError rc = CASS_OK;

    struct timeval t1,t2;
    gettimeofday(&t1, NULL);

    std::string query_str = "INSERT INTO nginx.t_nginx_log_by_day \
          (logdate, shardid, log_id, create_time, raw_data) \
          VALUES (?, ?, ?, ?, ?);" ;

    const char* query = query_str.c_str();
    int key_size = 5;

    int batch_size = 200;
    int vector_size = static_cast<int>(values.size());
    std::list<std::string>::iterator it;

    it = values.begin();

    for(int i = 0; i < vector_size; i += batch_size)
    {
        int batch_size_current = min(i + batch_size, vector_size);
        MYLOG_DEBUG(g_pLogger, "batch insert, from: %d, to: %d", i, batch_size_current);

        CassBatch* batch = cass_batch_new(CASS_BATCH_TYPE_LOGGED);

        // rand() % (max_number + 1 - minimum_number) + minimum_number
        int shardid = rand() % (11 + 1 - 0) + 0;

        for (int j = i; j < batch_size_current; j++)
        {
            const string& strRecord = *it;

            std::vector<std::string> elems;
            split(strRecord, '|', elems);
            if (elems.size() != 3)
            {
                MYLOG_WARN(g_pLogger, "Record Parse Fail, str=%s", strRecord.c_str());
                continue;
            }

            string log_time = elems[0];
            string raw_data = elems[1];

            MYLOG_DEBUG(g_pLogger, "log_type: %s, log_time:%s, raw_data: %s.", 
              log_type.c_str(), log_time.c_str(), raw_data.c_str());

            time_t timestamp_t;
            StringToTimeEX(log_time, timestamp_t);
            cass_int64_t timestamp_int64_t = (cass_int64_t)(timestamp_t * 1000);

            CassUuidGen* uuid_gen = cass_uuid_gen_new();
            CassUuid uuid;
            cass_uuid_gen_from_time(uuid_gen, timestamp_int64_t, &uuid);

            string logdate;
            TimeToStringEX(logdate, timestamp_t, 1);

            CassStatement* statement = cass_statement_new(query, key_size);

            cass_statement_bind_string(statement, 0, logdate.c_str());
            cass_statement_bind_int32(statement,  1, shardid);
            cass_statement_bind_uuid(statement,   2, uuid);
            cass_statement_bind_int64(statement,  3, timestamp_int64_t);
            cass_statement_bind_string(statement, 4, raw_data.c_str());

            cass_batch_add_statement(batch, statement);
            cass_uuid_gen_free(uuid_gen);
            cass_statement_free(statement);

            std::advance(it, 1);
        }

        CassFuture* batch_future = cass_session_execute_batch(_session, batch);
        cass_batch_free(batch);

        rc = cass_future_error_code(batch_future);
        if (rc != CASS_OK) 
        {
            print_error(batch_future);
        }

        cass_future_free(batch_future);
    }

    if(vector_size > batch_size)
    {
        gettimeofday(&t2, NULL);
        double t1_ms = (t1.tv_sec) * 1000 + (t1.tv_usec) / 1000 ;
        double t2_ms = (t2.tv_sec) * 1000 + (t2.tv_usec) / 1000 ; 
        double timespan = t2_ms - t1_ms;
        MYLOG_WARN(g_pLogger, "Write Cassandra Record size=%d cost[%0.2lf]ms", vector_size, timespan);
    }

    return rc;
}
