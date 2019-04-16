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

#include <commons/log.h>
#include <commons/string.h>
#include <commons/config.h>
#include <readline/readline.h>

#define PUERTO "6667"
#define BACKLOG 1			// Define cuantas conexiones vamos a mantener pendientes al mismo tiempo
#define MAX_PACKAGE_SIZE 1024	//El servidor no admitira paquetes de mas de 1024 bytes
#define MAX_MESSAGE_SIZE 300

#define CONFIG_PATH "MemorySocket.config"

t_log* g_logger;

char* serializarOperandos(t_Package*);
//void fill_package(t_Package*, char**);
void abrir_con(t_config **);
void dispose_package(char**);
int recieve_and_deserialize(t_Package *,int);

#endif
