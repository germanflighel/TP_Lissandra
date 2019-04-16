#ifndef PACKAGE_H_
#define PACKAGE_H_

#define SELECT 01
#define INSERT 02
#define DESCRIBE 03
#define DROP 04
#define CREATE 05
#define JOURNAL 06
#define RUN 07
#define ADD 08

#define SC 10
#define SHC 11
#define EC 12

/*
 * 	Definicion de estructuras
 *
 * 	Es importante destacar que utilizamos el tipo uint_32, incluida en el header <stdint.h> para mantener un estandar en la cantidad
 * 	de bytes del paquete.
 */

typedef struct t_Package {
	uint32_t message_long;
	char* message;
	uint32_t total_size;			// NOTA: Es calculable. Aca lo tenemos por fines didacticos!
} t_Package;

typedef struct t_Package_Select {
	int tipoPaquete;
	uint32_t nombreTabla_long;
	char* nombreTabla;
	uint32_t clave;
	uint32_t total_size;
} t_Package_Select;

typedef struct t_Package_Insert {
	int tipoPaquete;
	uint32_t nombreTabla_long;
	char* nombreTabla;
	uint32_t clave;
	char* valor;
	uint32_t valor_long;
	long int timestamp;
	uint32_t total_size;
} t_Package_Insert;

typedef struct t_Package_Describe {
	int tipoPaquete;
	uint32_t nombreTabla_long;
	char* nombreTabla;
	uint32_t total_size;
} t_Package_Describe;

typedef struct t_Package_Drop {
	int tipoPaquete;
	uint32_t nombreTabla_long;
	char* nombreTabla;
	uint32_t total_size;
} t_Package_Drop;

typedef struct t_Package_Create {
	int tipoPaquete;
	uint32_t nombreTabla_long;
	char* nombreTabla;
	int tipoConsistencia;
	uint32_t numero_Particiones;
	long int tiempo_Compactacion;
	uint32_t total_size;
} t_Package_Create;

typedef struct t_Package_Journal {
	int tipoPaquete;
	uint32_t total_size;
} t_Package_Journal;

typedef struct t_Package_Run {
	int tipoPaquete;
	uint32_t path_long;
	char* path;
	uint32_t total_size;
} t_Package_Run;

typedef struct t_Package_Add {
	int tipoPaquete;
	uint32_t numero;
	char* nombreTabla;
	int tipoConsistencia;
	uint32_t total_size;
} t_Package_Add;

#endif
