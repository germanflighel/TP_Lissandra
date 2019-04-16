#ifndef KERNEL_H_
#define KERNEL_H_

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>
#include <unistd.h>
#include <stdint.h>

#include "Package.h"
#include<commons/log.h>
#include<commons/string.h>
#include<commons/config.h>
#include<readline/readline.h>


#define IP "127.0.0.1"
#define PUERTO "6667"
#define MAX_MESSAGE_SIZE 300


char* serializarOperandos(t_Package*);
void fill_package(t_Package*);
void dispose_package(char**);
//int recieve_and_deserialize(t_Package *,int);

#endif
