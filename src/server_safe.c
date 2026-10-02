//
// Created by Sleyter Angulo on 9/17/26.
// Modificado por Bryan Jiménez para implementar Producer-Consumer
//

#include "../includes/net_util.h"
#include <errno.h>
#include <pthread.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>
#include <stdatomic.h>

#define _POSIX_C_SOURCE 200809L

#define DEFAULT_PORT 8080
#define LISTEN_BACKLOG 64
#define DRAIN_SECONDS 1
#define QUEUE_SIZE 256
#define NUM_WORKERS 4 // Hilos Consumers predeterminados

static volatile sig_atomic_t g_running = 1;
static atomic_ulong g_requests_served = 0;

typedef struct {
    int file_descriptor;
    unsigned long connection_id;
} connection_t;

// Buffer Circular (Queue)
connection_t *queue[QUEUE_SIZE];
int q_front = 0;
int q_rear = 0;
int q_count = 0;

pthread_mutex_t q_mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t q_not_empty = PTHREAD_COND_INITIALIZER;
pthread_cond_t q_not_full = PTHREAD_COND_INITIALIZER;

static void on_sigint(int signum)
{
    (void)signum;
    g_running = 0;
    // Despertar hilos bloqueados para salida limpia
    pthread_cond_broadcast(&q_not_empty); 
    pthread_cond_broadcast(&q_not_full);
}

static int install_signal_handlers(void)
{
    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = on_sigint;

    if (sigaction(SIGINT, &sa, NULL) < 0) {
        perror("sigaction SIGINT");
        return -1;
    }

    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = on_sigint; // Evita el cierre si el cliente corta conexión
    if (sigaction(SIGPIPE, &sa, NULL) < 0) {
        perror("sigaction SIGPIPE");
        return -1;
    }
    return 0;
}

//Hilos consumidores
static void *worker_thread(void *arg)
{
    (void)arg;
    while (g_running) {
        pthread_mutex_lock(&q_mutex);
        
        while (q_count == 0 && g_running) {
            pthread_cond_wait(&q_not_empty, &q_mutex);
        }
        
        if (!g_running && q_count == 0) {
            pthread_mutex_unlock(&q_mutex);
            break;
        }

        connection_t *conn = queue[q_front];
        q_front = (q_front + 1) % QUEUE_SIZE;
        q_count--;

        pthread_cond_signal(&q_not_full);
        pthread_mutex_unlock(&q_mutex);

        if (conn) {
            printf("[Handling connection %lu] accepted\n", conn->connection_id);
            fflush(stdout);

            if (nu_drain_request(conn->file_descriptor) < 0) {
                (void)nu_send_response(conn->file_descriptor, conn->connection_id);
            }

            atomic_fetch_add(&g_requests_served, 1);

            if (close(conn->file_descriptor) < 0)
                perror("close(file_descriptor)");

            free(conn);
        }
    }
    return NULL;
}

static unsigned short parse_port(int argc, char **argv)
{
    if (argc < 2) return DEFAULT_PORT;

    char *end = NULL;
    errno = 0;
    long value = strtol(argv[1], &end, 10);

    if (errno != 0 || end == argv[1] || *end != '\0' || value <= 0 || value > 65535) {
        fprintf(stderr, "invalid port '%s', using %d\n", argv[1], DEFAULT_PORT);
        return DEFAULT_PORT;
    }
    return (unsigned short)value;
}

int main(int argc, char **argv)
{
    if (install_signal_handlers() < 0) {
        return EXIT_FAILURE;
    }

    unsigned short port = parse_port(argc, argv);
    int listen_file_descriptor = nu_listen(port, LISTEN_BACKLOG);

    if (listen_file_descriptor < 0) {
        return EXIT_FAILURE;
    }

    // Instanciar el Thread Pool de Consumidores
    pthread_t workers[NUM_WORKERS];
    for (int i = 0; i < NUM_WORKERS; ++i) {
        if (pthread_create(&workers[i], NULL, worker_thread, NULL) != 0) {
            perror("pthread_create");
            return EXIT_FAILURE;
        }
    }

    printf("listening on port %u — Ctrl-C to stop (Pool size: %d)\n", port, NUM_WORKERS);
    fflush(stdout);

    unsigned long accepted = 0;

    // Productor
    while (g_running) {
        int client_file_descriptor = accept(listen_file_descriptor, NULL, NULL);
        if (client_file_descriptor < 0) {
            if (errno == EINTR) continue;
            perror("accept");
            break;
        }

        connection_t *conn = malloc(sizeof(connection_t));
        if (conn == NULL) {
            fprintf(stderr, "out of memory, dropping connection\n");
            close(client_file_descriptor);
            continue;
        }

        conn->file_descriptor = client_file_descriptor;
        conn->connection_id = ++accepted;

        pthread_mutex_lock(&q_mutex);
        while (q_count == QUEUE_SIZE && g_running) {
            pthread_cond_wait(&q_not_full, &q_mutex); // Bloquea si la cola está llena
        }

        if (!g_running) {
            pthread_mutex_unlock(&q_mutex);
            close(conn->file_descriptor);
            free(conn);
            break;
        }

        queue[q_rear] = conn;
        q_rear = (q_rear + 1) % QUEUE_SIZE;
        q_count++;

        pthread_cond_signal(&q_not_empty); // Despierta a un consumidor
        pthread_mutex_unlock(&q_mutex);
    }

    if (close(listen_file_descriptor)) {
        perror("close(listen_file_descriptor)");
    }

    // Espera a que los hilos consumidores terminen
    for (int i = 0; i < NUM_WORKERS; ++i) {
        pthread_join(workers[i], NULL);
    }

    sleep(DRAIN_SECONDS);

    printf("\naccepted: %lu\n", accepted);
    printf("served:   %lu\n", (unsigned long)g_requests_served);
    printf("lost:     %ld\n", (long)accepted - (long)g_requests_served);

    return EXIT_SUCCESS;
}