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

#include <commons/collections/list.h>
#include <commons/log.h>
#include <commons/string.h>
#include <commons/config.h>
#include <readline/readline.h>
#include <pthread.h>

#define MAX_VALUE_SIZE 10
#define MEMORY_SIZE 5000

#define PUERTO "6667"
#define BACKLOG 1			// Define cuantas conexiones vamos a mantener pendientes al mismo tiempo

#define CONFIG_PATH "MemorySocket.config"

typedef struct Pagina {
	long timeStamp;
	uint16_t key;
	char value[MAX_VALUE_SIZE];
} Pagina;

typedef struct Renglon_pagina {
	uint32_t numero;
	char modificado;
	uint32_t offset;
} Renglon_pagina;

typedef struct Tabla_paginas {
	Renglon_pagina* renglones;
}Tabla_paginas;

typedef struct Segmento {
	char path[50];
	t_list* numeros_pagina;
}Segmento;


t_log* g_logger;

void abrir_con(t_config **);
void send_package(int header, void* package, int lfsSocket);

#endif
