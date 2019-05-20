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
#include <sockets.h>

#define BACKLOG 1

#define CONFIG_PATH "LFSSocket.config"
#define LOG_FILE_PATH "lfs_global.log"

typedef struct Reg {
	long int timeStamp;
	int key;
	char* value;
};

typedef struct Metadata {
	int consistency;
	int partitions;
	long int compaction_time;
    char nombre_tabla[MAX_TABLE_LENGTH];
}Metadata;

t_config* leer_config();

t_log* iniciar_logger();

void lfs_select(t_PackageSelect* package, char* ruta);
void lfs_insert(t_PackageInsert* package);
int existe_tabla(char* nombre_tabla, char** ruta);
Metadata* obtener_metadata(char* ruta);
char* consistency_to_str(int consistency);
t_list* lfs_describe(char* punto_montaje);
void loguear_metadata(Metadata* metadata);
//int calcular_particion(int key,int cantidad_particiones);
//char* obtener_nombre_tabla(char** parametros_separados);
//void obtener_nombre_tabla(char*,char**);

#endif
