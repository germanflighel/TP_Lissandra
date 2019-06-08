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
#include "sockets.h"

#include<commons/log.h>
#include<commons/string.h>
#include<commons/config.h>
#include<readline/readline.h>
#include <commons/collections/list.h>
#include <commons/collections/queue.h>
#include <commons/string.h>
#include <commons/collections/dictionary.h>


#define CONFIG_PATH "KernelSocket.config"
#define LOG_FILE_PATH "kernel_global.log"
#define CORTE_SCRIPT_POR_LINEA_ERRONEA -10
#define CORTE_SCRIPT_POR_FIN_QUANTUM -9
#define CORTE_SCRIPT_POR_FINALIZACION -8


t_list* tablas_actuales;
typedef struct Tabla {
	char nombre_tabla[MAX_TABLE_LENGTH];
	uint8_t consistencia;
} Tabla;

typedef struct Memoria {
	int socket;
	int numero;
	int puerto;
} Memoria;

typedef struct Script {
	int index;
	int cant_lineas;
	char** lineas;
} Script;

t_list* memoriasConectadas;

t_log* iniciar_logger(void);
void abrir_config(t_config **);

int is_regular_file(const char*);

int interpretarComando(int,char*,int);
int insert_kernel(char*,int);
void describe(char*,int);
void drop(char*,int);
void create(char*,int);
void journal(char*,int);
int run(char*,int);
void add(char*,int);
void metrics(char*,int);
int select_kernel(char*,int);
Script* levantar_script(char*);
void* exec(int);
void* intentarEstablecerConexion();


#endif
