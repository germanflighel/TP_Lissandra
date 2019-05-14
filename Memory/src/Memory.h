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

#include <commons/log.h>
#include <commons/string.h>
#include <commons/config.h>
#include <readline/readline.h>
#include <pthread.h>

#define PUERTO "6667"
#define BACKLOG 1			// Define cuantas conexiones vamos a mantener pendientes al mismo tiempo

#define CONFIG_PATH "MemorySocket.config"

t_log* g_logger;

void abrir_con(t_config **);
void send_package(int header, void* package, int lfsSocket);

#endif
