#include <string>
#include <vector>
#include <stdio.h>
#include <getopt.h>
#include <time.h>
#include <stdlib.h>
#include <unistd.h>
#include <pthread.h> 
#include <errno.h>
#include <stdint.h>
#include <signal.h>
#include <sys/time.h>
#include <sys/uio.h>
#include "perfsk.h"

using namespace std;

static char VERSION[] = "0.1";

static struct config 
{
    int connections;
    int duration;
    int threads;
    string host;
    string port;
} cfg;

static uint64_t time_us() {
    struct timeval t;
    gettimeofday(&t, NULL);
    return (t.tv_sec * 1000000) + t.tv_usec;
}

static volatile sig_atomic_t stop = 0;

// static void handler(int sig) {
//     stop = 1;
// }

static void usage() {
    printf("Usage: perfsk <options>                               \n"
           "  Options:                                            \n"
           "    -c, --connections <N>  Connections to keep open   \n"
           "    -d, --duration    <T>  Duration of test           \n"
           "    -t, --threads     <N>  Number of threads to use   \n"
           "    -h, --host        <S>  Host IP address of Server  \n"
           "    -p, --port        <S>  Port of Server             \n"
           "    -v, --version          Print version              \n");
}

static struct option longopts[] = 
{
    { "connections", required_argument, NULL, 'c' },
    { "duration",    required_argument, NULL, 'd' },
    { "threads",     required_argument, NULL, 't' },
    { "host",        required_argument, NULL, 'h' },
    { "port",        required_argument, NULL, 'p' },
    { "help",        no_argument,       NULL, 'H' },
    { "version",     no_argument,       NULL, 'v' },
    { NULL,          0,                 NULL,  0  }
};

static int parse_args(struct config *cfg, int argc, char **argv) 
{
    int c;

    cfg->threads     = 2;
    cfg->connections = 10;
    cfg->duration    = 10;

    while ((c = getopt_long(argc, argv, "c:d:t:h:p:Hv?", longopts, NULL)) != -1) 
    {
        switch (c) {
            case 't':
                cfg->threads = atoi(optarg);
                break;
            case 'c':
                cfg->connections = atoi(optarg);
                break;
            case 'd':
                cfg->duration = atoi(optarg);
                break;
            case 'h':
                cfg->host = optarg;
                break;
            case 'p':
                cfg->port = optarg;
                break;
            case 'v':
                printf("perfsk %s\n", VERSION);
                break;
            case 'H':
            case '?':
            case ':':
            default:
                return -1;
        }
    }

    if (!cfg->connections || cfg->connections < cfg->threads) 
    {
        fprintf(stderr, "number of connections must be >= threads\n");
        return -1;
    }

    return 0;
}


void *thread_main(void *arg) 
{ 
    thread *th = (thread *)arg;
    th->stat.requests = 0; 
    th->start = time_us();

    while ( true )
    {
        sleep(1);

        th->stat.requests += 1; 

        if (stop) break;
    }

    printf("ID: %d, requests: %d\n", th->query.uid, th->stat.requests); 

    return NULL;
}  

int main(int argc, char *argv[])
{
    if (parse_args(&cfg, argc, argv)) 
    {
        usage();
        exit(1);
    }

    printf("args: connections[%d], duration[%d], threads[%d], host[%s], port[%s]\n", 
        cfg.connections, cfg.duration, cfg.threads, cfg.host.c_str(), cfg.port.c_str());

    int thread_num = cfg.threads;
    thread *threads     = (thread *)(malloc(thread_num * sizeof(thread)));

    for (int i = 0; i < thread_num; i++)
    {
        thread *t      = &threads[i];

        // rand() % (max_number + 1 - minimum_number) + minimum_number
        t->query.uid = rand() % (1000000 + 1 - 1) + 1;

        pthread_create(&t->thread, NULL, thread_main, t); 
    }

    uint64_t start = time_us();

    sleep(cfg.duration);
    stop = 1;

    uint64_t total_requests = 0;
    for (int i = 0; i < thread_num; i++) 
    {
        thread *t = &threads[i];
        pthread_join(t->thread, NULL);

        total_requests += t->stat.requests;
    }

    uint64_t runtime_us = time_us() - start;
    long double runtime_s = runtime_us / 1000000.0;
    long double req_per_s = total_requests / runtime_s;

    printf("Requests/sec: %9.2Lf\n", req_per_s);

    pthread_exit(NULL); 

    free(threads);
	
    return 0;
}



