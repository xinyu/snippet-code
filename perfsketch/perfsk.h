#ifndef PERFSK_H
#define PERFSK_H

#include <pthread.h>
#include <inttypes.h>
#include <sys/types.h>
#include <netdb.h>


typedef struct {
    int uid;
    int read;
} CUST_QUERY;

typedef struct {
    int latency_avg;
    int latency_max;
    int latency_min;
    int requests;
    int run_time;
    int req_per_s;
} STAT_DATA;

typedef struct {
    pthread_t thread;
    int connections;
    int complete;
    int requests;
    int bytes;
    uint64_t start;
    CUST_QUERY query; 
    STAT_DATA  stat;
} thread;


#endif /* PERFSK_H */
