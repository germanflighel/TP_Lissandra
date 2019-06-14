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
#include <math.h>
#include "sockets.h"


#include <commons/collections/list.h>
#include <commons/log.h>
#include <commons/string.h>
#include <commons/config.h>
#include <readline/readline.h>
#include <pthread.h>
#include <readline/history.h>


#define BACKLOG 1			// Define cuantas conexiones vamos a mantener pendientes al mismo tiempo



/*
typedef struct Pagina {
	long timeStamp;
	uint16_t key;
	char value[MAX_VALUE_SIZE];
} Pagina;
*/

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
Segmento* buscarSegmento(char* tabla);
void* buscarPagina(int key,Segmento* segmento,int* num_pag);
int ejecutarSelect(t_PackageSelect* select, int lfs_socket);

#endif
