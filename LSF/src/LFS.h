#ifndef MEMORY_H_
#define MEMORY_H_

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/inotify.h>
#include <netdb.h>
#include <unistd.h>
#include <stdint.h>
#include <pthread.h>

#include <commons/log.h>
#include <commons/string.h>
#include <commons/config.h>
#include <commons/bitarray.h>
#include <readline/readline.h>
#include <readline/history.h>
#include <ftw.h>
#include <stdarg.h>

#include <sockets.h>

#define BACKLOG 1

#define INFO -30
#define DEBUG -29
#define WARNING -28

#define CONFIG_PATH "LFSSocket.config"
#define LOG_FILE_PATH "lfs_global.log"


#define MAX_EVENTS 1024 /*Max. number of events to process at one go*/
#define LEN_NAME 1024 /*Assuming length of the filename won't exceed 16 bytes*/
#define EVENT_SIZE  ( sizeof (struct inotify_event) ) /*size of one event*/
#define BUF_LEN     ( MAX_EVENTS * ( EVENT_SIZE + LEN_NAME ))

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

void* recibir_por_consola();
void* compactar_tabla(Metadata* una_tabla);
Registro* lfs_select(t_PackageSelect* package);
void* ejecutar_comando(int header, void* package);


int lfs_insert(t_PackageInsert* package);

int existe_tabla(char* tabla);
Metadata* obtener_metadata(char* ruta);
char* consistency_to_str(int consistency);
t_list* lfs_describe();
void loguear_metadata(Metadata* metadata);
void loguear_registro(Registro* registro);
int calcular_particion(int key,int cantidad_particiones);
Registro* buscar_en_mem_table(char* nombre_tabla, int keyBuscada);
Registro* encontrar_keys(int keyBuscada, int particion_objetivo, char* ruta_a_tabla, char* nombre_tabla);
void loguear_int(int n);
int lfs_create(t_PackageCreate* package);
char* ruta_a_tabla(char* tabla);
int agregar_tabla_a_mem_table(char* tabla);
int insertar_en_mem_table(Registro* registro_a_insertar, char* nombre_tabla);
t_list* lfs_describe_a_table(char* nombre_tabla);
char* contenido_de_los_bloques(char* nombre_tabla, char** blocks);
void* dump();

char* contenido_de_temporales(char* nombre_tabla, double* tiempo_bloqueado);
t_list* obtener_diferencias(char* nombre_tabla, int particiones, char* contenido_temporal);

void escribir_registros_en_bloques(Tabla* tabla);
char* blocks_to_string(t_list* blocks);

char* leer_registros_de(char* nombre_tabla, char* extension);

void* watch_config(char*);

void _mostrar_metadata(Metadata* metadata);
#endif
