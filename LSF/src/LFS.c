/*
 ============================================================================
 Name        : LFS.c
 Author      : 
 Version     :
 Copyright   : Your copyright notice
 Description : Hello World in C, Ansi-style
 ============================================================================
 */

#include "LFS.h"
#include "sockets.h"
#include <dirent.h>
#include <commons/collections/dictionary.h>
#include <commons/collections/list.h>

t_log* logger;

int main() {

	struct addrinfo hints;
	struct addrinfo *serverInfo;

	logger = iniciar_logger();
	t_config* config = leer_config();

	char* puerto = config_get_string_value(config, "PUERTO_ESCUCHA");
	char* ruta = config_get_string_value(config, "PUNTO_MONTAJE");
	log_debug(logger, puerto);

	memset(&hints, 0, sizeof(hints));
	hints.ai_family = AF_UNSPEC;
	hints.ai_flags = AI_PASSIVE; // Asigna el address del localhost: 127.0.0.1
	hints.ai_socktype = SOCK_STREAM;	// Indica que usaremos el protocolo TCP

	getaddrinfo(NULL, puerto, &hints, &serverInfo); // Notar que le pasamos NULL como IP, ya que le indicamos que use localhost en AI_PASSIVE

	int listenningSocket;
	listenningSocket = socket(serverInfo->ai_family, serverInfo->ai_socktype,
			serverInfo->ai_protocol);

	bind(listenningSocket, serverInfo->ai_addr, serverInfo->ai_addrlen);
	freeaddrinfo(serverInfo); // Ya no lao vamos a necesitar

	listen(listenningSocket, BACKLOG); // IMPORTANTE: listen() es una syscall BLOQUEANTE.

	printf("Esperando memoria... \n");

	struct sockaddr_in addr; // Esta estructura contendra los datos de la conexion del cliente. IP, puerto, etc.
	socklen_t addrlen = sizeof(addr);

	int socketCliente = accept(listenningSocket, (struct sockaddr *) &addr,
			&addrlen);

	if (!recibir_handshake(MEMORY, socketCliente)) {
		printf("Handshake invalido \n");
		return 0;
	}

	lfs_describe(ruta);

	t_PackagePosta package;
	int status = 1;		// Estructura que maneja el status de los recieve.

	printf("Memoria conectada. Esperando Envio de mensajes.\n");

	int headerRecibido;

	while (status) {

		log_debug(logger, "Header");
		headerRecibido = recieve_header(socketCliente);
		printf("End Header. \n");
		log_debug(logger, "Status");

		status = headerRecibido;

		if (status) {
			if (headerRecibido == SELECT) {

				log_debug(logger, "Got a SELECT");

				t_PackageSelect package;
				status = recieve_and_deserialize_select(&package, socketCliente);

				ejecutar_comando(headerRecibido, &package, ruta);

			} else if (headerRecibido == INSERT) {

				log_debug(logger, "Got an INSERT");

				t_PackageInsert package;
				status = recieve_and_deserialize_insert(&package, socketCliente);

				ejecutar_comando(headerRecibido, &package, ruta);

			} /*else if (headerRecibido == DESCRIBE) {

				log_debug(logger, "Got an DESCRIBE");

				t_describe package;


			}*/

		}

	}

	printf("Cliente Desconectado.\n");

	close(socketCliente);
	close(listenningSocket);

	log_destroy(logger);
	config_destroy(config);

	return 0;
}

t_config* leer_config() {
	return config_create(CONFIG_PATH);
}

t_log* iniciar_logger(void) {

	return log_create(LOG_FILE_PATH, "LFS", 1, LOG_LEVEL_DEBUG);

}

void ejecutar_comando(int header, void* package, char* ruta) {
	switch (header) {
	case SELECT:
		lfs_select((t_PackageSelect*) package, ruta);
		break;
	case INSERT:
		lfs_insert((t_PackageInsert*) package);
		break;
	}
}

//Falta agregar funcionalidad de que debe buscar a la tabla correspondiente el valor y demas...
void lfs_select(t_PackageSelect* package, char* ruta) {


	printf("Antes de append \n");

	char* mi_ruta = string_duplicate(ruta);

	log_debug(logger, mi_ruta);

	printf("A ver si existe tabla \n");

	if (!existe_tabla(package->tabla, &mi_ruta)) {
		log_debug(logger, "No existe la tabla");
		return;
	}
	printf("Antes existe \n");
	log_debug(logger, "Existe tabla, BRO!");
	//2) Obtener Metadata
	t_dictionary* metadata = dictionary_create();

	//obtener_metadata(mi_ruta);

	log_debug(logger, (char*) dictionary_get(metadata, "consistencia"));
	log_debug(logger,
			string_itoa(dictionary_get(metadata, "particiones")));
	log_debug(logger,
			string_itoa(dictionary_get(metadata, "tiempoDeCompactacion")));
	//3) Calcular que particion contiene a KEY
	int particionObjetivo = calcular_particion(package->key,
			dictionary_get(metadata, "particiones"));
	log_debug(logger, string_itoa(particionObjetivo));

	//4) Escanear particion objetivo, archivos temporales y memoria temporal
	//t_list* valuesEncontrados = list_create();
	//encontrar_keys(key, particionObjetivo, valuesEncontrados);
	//5) Devolver o mostrar el valor mayor
	//log_debug(logger, maximoTimestamp(valuesEncontrados));
	//struct Reg *reg = list_get(valuesEncontrados, 0);
	//log_debug(logger, reg->value);

	free(mi_ruta);

	log_debug(logger, "Going back");
	printf("hola \n");
}

void lfs_insert(t_PackageInsert* package) {


}

void lfs_describe(char* punto_montaje){

	DIR *tables_directory;
	struct dirent *a_directory;
	char* tablas_path = string_new();
	string_append(&tablas_path, punto_montaje);
	string_append(&tablas_path, "/tables/");
	log_debug(logger, tablas_path);
	tables_directory = opendir(tablas_path);
	if (tables_directory) {
		while ((a_directory = readdir(tables_directory)) != NULL) {
			if (strcmp(a_directory->d_name, ".") && strcmp(a_directory->d_name, "..")) {
				Metadata metadata;
				char* a_table_path = string_new();
				char* table_name = malloc(strlen(a_directory->d_name));
				memcpy(table_name, a_directory->d_name,strlen(a_directory->d_name) + 1);
				string_append(&a_table_path, tablas_path);
				string_append(&a_table_path, table_name);
				log_error(logger, a_table_path);
				log_debug(logger, "Voy a obtener metadata");
				obtener_metadata(&metadata, a_table_path);
				loguear_metadata(&metadata);
				free(a_table_path);
			}

		}
		closedir(tables_directory);
	}
}

int existe_tabla(char* nombre_tabla, char** ruta) {

	char* tables = "/tables/";
	printf("Me rompi aca \n");
	string_append(ruta, tables);
	string_append(ruta, nombre_tabla);
	printf("Nono, me rompi aca papi \n");
	int status=1;
	log_debug(logger, *ruta);
	DIR *dirp;
	dirp = opendir(*ruta);

	if (dirp == NULL) {
		status= 0;
	}
	closedir(dirp);
	return status;
}

void loguear_metadata(Metadata* metadata) {

	log_debug(logger, consistency_to_str(metadata->consistency));
	log_debug(logger, string_itoa(metadata->partitions));
	log_debug(logger, string_itoa(metadata->compaction_time));
}

void obtener_metadata(Metadata* metadata, char* ruta) {
	char* mi_ruta = string_new();
	string_append(&mi_ruta, ruta);
	char* mi_metadata = "/Metadata";
	string_append(&mi_ruta, mi_metadata);
	log_debug(logger,mi_ruta);

	t_config* config_metadata = config_create(mi_ruta);
	log_debug(logger,"Voy a leer las particiones");
	int particiones = config_get_int_value(config_metadata, "PARTITIONS");
	metadata->partitions = particiones;
	log_debug(logger,"Voy a leer el tiempo de compactacion");
	long tiempoDeCompactacion = config_get_long_value(config_metadata,"COMPACTION_TIME");
	metadata->compaction_time = tiempoDeCompactacion;
	char* consistencia = malloc(4 * sizeof(char));
	log_debug(logger,"Voy a leer la consistencia");
	char* temp = config_get_string_value(config_metadata, "CONSISTENCY");
	memcpy(consistencia, temp, 4 * sizeof(char));
	metadata->consistency = consistency_to_int(consistencia);
	log_debug(logger,"Lei todo papa");
	free(temp);
	free(consistencia);

	loguear_metadata(metadata);
	config_destroy(config_metadata);
	free(mi_ruta);

}

int calcular_particion(int key, int cantidad_particiones) {

	return (key % cantidad_particiones) + 1;
}

void encontrar_keys(int keyBuscada, int particion_objetivo,
		t_list* lista_values) {

	char* ruta = string_new();
	string_append(&ruta, "t1/1.bin");
	FILE* lector = fopen(ruta, "rb");
	struct Reg reg;
	while (!feof(lector)) {
		fread(&reg, sizeof(reg), 1, lector);
		printf(reg.value);
		if (reg.key == keyBuscada) {
			list_add(lista_values, &reg);
		}
	}

}

int consistency_to_int(char* consistency){
	if(strcmp(consistency, "SC") == 0){
		return SC;
	}
	else if(strcmp(consistency, "SHC") == 0){
		return SHC;
	}
	else if(strcmp(consistency, "EC") == 0){
		return EC;
	}
}

char* consistency_to_str(int consistency){
	switch(consistency){
		case SC:
			return "SC";
		case SHC:
			return "SHC";
		case EC:
			return "EC";
	}
}

//Preguntar que onda esta opcion, si pierdo la referencia al hacer malloc y devolverlo. Comparar con la otra funcion de abajo

/*char* obtener_nombre_tabla(char** parametros_separados){
 char* nombre_tabla = malloc(strlen(parametros_separados[0]));
 strcpy(nombre_tabla,parametros_separados[0]);
 return nombre_tabla;
 }
 */

/*void obtener_nombre_tabla(char* nombre_tabla, char** parametros_separados){
 nombre_tabla = malloc(strlen(parametros_separados[0]));
 strcpy(nombre_tabla,parametros_separados[0]);
 }*/
