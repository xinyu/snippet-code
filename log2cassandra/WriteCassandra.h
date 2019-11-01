#ifndef _WRITE_CASSANDRA_H_
#define _WRITE_CASSANDRA_H_

#include <string>
#include <vector>
#include "cassandra.h"

using namespace std;

class WriteCassandra
{
public:
    WriteCassandra()
    {
        _cluster = NULL;
        _session = cass_session_new();
        _close_future = NULL;
    };
    
    ~WriteCassandra()
    { 
        close();
    };

   void print_error(CassFuture* future);
   bool init();
   void close();

   bool create_cluster();
   CassError connect_session();
   CassError execute_query(const char* query);

   CassError insert_log_batch(std::list<std::string>& values);

   CassError insert_nginxlog_by_id(std::list<std::string>& values);
   CassError insert_nginxlog_by_day(std::list<std::string>& values);

private:

    CassCluster* _cluster;
    CassSession* _session;
    CassFuture* _close_future;

};


#endif  /* _WRITE_CASSANDRA_H_ */

