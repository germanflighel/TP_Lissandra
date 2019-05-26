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
#include <pthread.h>

#include <commons/log.h>
#include <commons/string.h>
#include <commons/config.h>
#include <readline/readline.h>
#include <sockets.h>

#define BACKLOG 1

#define CONFIG_PATH "LFSSocket.config"
#define LOG_FILE_PATH "lfs_global.log"

typedef struct Registro {
	long timeStamp;
	int key;
	char* value;
} Registro;

typedef struct Tabla {
	char nombre_tabla[MAX_TABLE_LENGTH];
	t_list* registros;
} Tabla;


typedef struct Metadata {
	int consistency;
	int partitions;
	long int compaction_time;
    char nombre_tabla[MAX_TABLE_LENGTH];
}Metadata;

t_config* leer_config();

t_log* iniciar_logger();

Registro* lfs_select(t_PackageSelect* package, char* ruta);
void* ejecutar_comando(int header, void* package, char* ruta);

int lfs_insert(t_PackageInsert* package, char* ruta);

int existe_tabla(char* tabla);
Metadata* obtener_metadata(char* ruta);
char* consistency_to_str(int consistency);
t_list* lfs_describe(char* punto_montaje);
void loguear_metadata(Metadata* metadata);
void loguear_registro(Registro* registro);
int calcular_particion(int key,int cantidad_particiones);

Registro* encontrar_keys(int keyBuscada, int particion_objetivo, char* ruta, char* montaje);
void loguear_int(int n);

int agregar_tabla_a_mem_table(char* tabla);
int insertar_en_mem_table(Registro* registro_a_insertar, char* nombre_tabla);

#endif
