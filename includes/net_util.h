//
// Created by Sleyter Angulo on 9/14/26.
//

#ifndef MINI_SERVER_NET_UTIL_H
#define MINI_SERVER_NET_UTIL_H

#include <stddef.h>
#include <sys/types.h>

int nu_listen(unsigned short port, int backlog);

int nu_write_all(int file_descriptor, const void *buffer, size_t size);

ssize_t nu_drain_request(int file_descriptor);

int nu_send_response(int file_descriptor, unsigned long connection_id);

#endif //MINI_SERVER_NET_UTIL_H