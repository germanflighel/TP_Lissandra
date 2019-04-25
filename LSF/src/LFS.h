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

#define BACKLOG 1

#define CONFIG_PATH "LFSSocket.config"
#define LOG_FILE_PATH "lfs_global.log"

t_config* leer_config();

t_log* iniciar_logger();

void interpretar_parametros(int header,char* parametros);
void lfs_select(char* parametros);
void lfs_insert(char* parametros);
//int existe_tabla(char* nombre_tabla);
//void obtener_metadata(char* tabla, t_dictionary* metadata);
//int calcular_particion(int key,int cantidad_particiones);
//char* obtener_nombre_tabla(char** parametros_separados);
//void obtener_nombre_tabla(char*,char**);

#endif
