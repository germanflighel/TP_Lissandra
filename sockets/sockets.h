#ifndef SOCKETS_H_
#define SOCKETS_H_

#define SELECT 1
#define INSERT 2
#define DESCRIBE 3
#define DROP 4
#define CREATE 5
#define JOURNAL 6
#define RUN 7
#define ADD 8
#define ERROR 9

#define SC 10
#define SHC 11
#define EC 12

#define MAX_MESSAGE_SIZE 300

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>
#include <unistd.h>
#include <stdint.h>
#include <commons/collections/list.h>
#include <commons/string.h>

typedef struct t_PackagePosta {
	uint32_t header;
	uint32_t message_long;
	char* message;
	uint32_t total_size;			// NOTA: Es calculable. Aca lo tenemos por fines didacticos!
} t_PackagePosta;




typedef struct t_PackageSelect {
	uint32_t header;
	uint32_t tabla_long;
	char* tabla;
	uint16_t key;
	uint32_t total_size;
} t_PackageSelect;

typedef struct t_PackageInsert {
	uint32_t header;
	uint32_t tabla_long;
	char* tabla;
	uint16_t key;
	uint32_t total_size;
	uint32_t value_long;
	char* value;
	long timestamp;
} t_PackageInsert;

#define LFS 100
#define MEMORY 101
#define KERNEL 102
#define HANDSHAKE 103

typedef struct t_Handshake {
	uint32_t header;
	uint32_t id;
} t_Handshake;

char* serializarOperandos(t_PackagePosta*);
void fill_package(t_PackagePosta*);
void dispose_package(char**);
int recieve_and_deserialize(t_PackagePosta*, int);
char* serializarHandShake(t_Handshake*);

#endif
