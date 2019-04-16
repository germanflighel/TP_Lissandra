#ifndef MEMORY_H_
#define MEMORY_H_

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>
#include <unistd.h>
#include <stdint.h>
#include "Package.h"

#define PUERTO "6668"
#define BACKLOG 1			// Define cuantas conexiones vamos a mantener pendientes al mismo tiempo
#define MAX_PACKAGE_SIZE 1024	//El servidor no admitira paquetes de mas de 1024 bytes
#define MAX_MESSAGE_SIZE 300


//char* serializarOperandos(t_Package*);
//void fill_package(t_Package*, char**);
int recieve_and_deserialize(t_Package *,int);

#endif
