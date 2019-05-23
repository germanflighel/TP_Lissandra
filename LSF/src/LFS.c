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
#include <unistd.h>
#include <fcntl.h>
#include <sys/mman.h>
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

	int max_value_size = config_get_int_value(config,"TAMAÑO_VALUE");
	log_debug(logger,string_itoa(max_value_size));
	send(socketCliente, &max_value_size,sizeof(u_int16_t), 0);

	t_list* metadatas = lfs_describe(ruta);
	list_iterate(metadatas, (void*) loguear_metadata);

	t_describe* describe = malloc(sizeof(t_describe));
	int cantidad_de_tablas = metadatas->elements_count;
	describe->cant_tablas = cantidad_de_tablas;
	describe->tablas = malloc(cantidad_de_tablas * sizeof(t_metadata));

	for (int i = 0;  i < cantidad_de_tablas; i++) {
		Metadata* a_metadata = (Metadata*) list_get(metadatas, i);
		t_metadata* meta = malloc(sizeof(t_metadata));
		meta->consistencia = a_metadata->consistency;
		strcpy(meta->nombre_tabla, a_metadata->nombre_tabla);

		describe->tablas[i] = *meta;
	}


	char* serializedPackage;
	serializedPackage = serializarDescribe(describe);
	log_debug(logger, string_itoa(cantidad_de_tablas));
	send(socketCliente, serializedPackage, cantidad_de_tablas*sizeof(t_metadata) + sizeof(describe->cant_tablas), 0);
	dispose_package(&serializedPackage);


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

				Registro* registro_a_devolver = (Registro*) ejecutar_comando(headerRecibido, &package, ruta);

				t_Respuesta_Select respuesta;

				if(registro_a_devolver->value) {
					respuesta.result = 1;
					respuesta.value = malloc(strlen(registro_a_devolver->value));
					strcpy(respuesta.value,registro_a_devolver->value);
					respuesta.value_long = strlen(respuesta.value);
				} else {
					respuesta.result = 0;
					respuesta.value = NULL;
					respuesta.value_long = 0;
				}

				send(socketCliente, &respuesta, sizeof(respuesta));
			} else if (headerRecibido == INSERT) {

				log_debug(logger, "Got an INSERT");

				t_PackageInsert package;
				status = recieve_and_deserialize_insert(&package, socketCliente);

				//ejecutar_comando(headerRecibido, &package, ruta);

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

void* ejecutar_comando(int header, void* package, char* ruta) {
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
Registro* lfs_select(t_PackageSelect* package, char* ruta) {

	char* mi_ruta = string_new();
	string_append(&mi_ruta,ruta);

	log_debug(logger, mi_ruta);

	char* tables = "/Tables/";
	string_append(&mi_ruta, tables);
	string_append(&mi_ruta, package->tabla);

	log_debug(logger, mi_ruta);

	if (!existe_tabla(mi_ruta)) {
		log_debug(logger, "No existe la tabla");
		return;
	}
	log_debug(logger, "Existe tabla, BRO!");

	//2) Obtener Metadata
	Metadata* metadata = obtener_metadata(mi_ruta);

	loguear_metadata(metadata);

	//3) Calcular que particion contiene a KEY
	int particionObjetivo = calcular_particion(package->key, metadata->partitions);
	log_debug(logger, string_itoa(particionObjetivo));

	//4) Escanear particion objetivo, archivos temporales y memoria temporal
	Registro* registro_mayor  = encontrar_keys(package->key, particionObjetivo, mi_ruta, ruta);


	if(registro_mayor->value) {
		log_debug(logger, "El value correspondiente al mayor timeStamp es: ");
		log_debug(logger, registro_mayor->value);
	} else {
		log_debug(logger, "No existe un registro con esa key");
	}



	free(metadata);
	free(mi_ruta);

	return registro_mayor;
}

void lfs_insert(t_PackageInsert* package) {


}

t_list* lfs_describe(char* punto_montaje){

	t_list* metadatas = list_create();
	DIR *tables_directory;
	struct dirent *a_directory;
	char* tablas_path = string_new();
	string_append(&tablas_path, punto_montaje);
	string_append(&tablas_path, "/Tables/");
	log_debug(logger, tablas_path);
	tables_directory = opendir(tablas_path);
	if (tables_directory) {
		while ((a_directory = readdir(tables_directory)) != NULL) {
			if (strcmp(a_directory->d_name, ".") && strcmp(a_directory->d_name, "..")) {
				char* a_table_path = string_new();
				char* table_name = malloc(strlen(a_directory->d_name));
				memcpy(table_name, a_directory->d_name,strlen(a_directory->d_name) + 1);
				string_append(&a_table_path, tablas_path);
				string_append(&a_table_path, table_name);
				Metadata* metadata = obtener_metadata(a_table_path);
				strcpy(metadata->nombre_tabla, table_name);
				list_add(metadatas, metadata);
				free(table_name);
				free(a_table_path);
			}

		}
		closedir(tables_directory);
	}
	return metadatas;
}


int existe_tabla(char* tabla) {
	log_debug(logger, tabla);

	int status=1;
	DIR *dirp;

	dirp = opendir(tabla);

	if (dirp == NULL) {
		status= 0;
	}
	closedir(dirp);
	return status;
}



void loguear_metadata(Metadata* metadata) {
	//log_debug(logger, metadata->nombre_tabla);
	log_debug(logger, consistency_to_str(metadata->consistency));
	log_debug(logger, string_itoa(metadata->partitions));
	log_debug(logger, string_itoa(metadata->compaction_time));
}

void loguear_registro(Registro* registro) {
	log_debug(logger, string_itoa(registro->timeStamp));
	log_debug(logger, string_itoa(registro->key));
	log_debug(logger, registro->value);
}


Metadata* obtener_metadata(char* ruta) {
	char* mi_ruta = string_new();
	string_append(&mi_ruta, ruta);
	char* mi_metadata = "/Metadata";
	string_append(&mi_ruta, mi_metadata);
	log_debug(logger, mi_ruta);

	t_config* config_metadata = config_create(mi_ruta);

	Metadata* metadata = malloc(sizeof(Metadata));
	int particiones = config_get_int_value(config_metadata, "PARTITIONS");

	metadata->partitions = particiones;
	long tiempoDeCompactacion = config_get_long_value(config_metadata,"COMPACTION_TIME");
	metadata->compaction_time = tiempoDeCompactacion;


	char* consistencia = config_get_string_value(config_metadata, "CONSISTENCY");
	metadata->consistency = consistency_to_int(consistencia);

	config_destroy(config_metadata);
	free(mi_ruta);

	return metadata;
}

int calcular_particion(int key, int cantidad_particiones) {

	return (key % cantidad_particiones) + 1;
}

Registro* encontrar_keys(int keyBuscada, int particion_objetivo, char* ruta, char* montaje) {
	char* mi_ruta = string_new();
	string_append(&mi_ruta, ruta);
	char* barra = "/";
	string_append(&mi_ruta, barra);
	string_append(&mi_ruta, string_itoa(particion_objetivo));
	char* bin = ".bin";
	string_append(&mi_ruta, bin);

	log_debug(logger, mi_ruta);

	log_warning(logger, "Ahora voy a mostrar los datos de los bloques");

	t_config* particion = config_create(mi_ruta);

	int size = config_get_int_value(particion, "SIZE");
	char** blocks = config_get_array_value(particion, "BLOCKS");

	Registro* registro = malloc(sizeof(Registro));
	registro->timeStamp = 0;
	int i = 0;
	while(blocks[i] != NULL){
		char* ruta_a_bloque = string_new();
		string_append(&ruta_a_bloque, montaje);
		string_append(&ruta_a_bloque, "/Bloques/");
		string_append(&ruta_a_bloque, blocks[i]);
		string_append(&ruta_a_bloque, ".bin");

		log_debug(logger, ruta_a_bloque);

		int fd = open(ruta_a_bloque, O_RDONLY, S_IRUSR | S_IWUSR);

		log_debug(logger, "Abri el Bloque");

		struct stat s;
	    int status = fstat (fd, & s);
	    size = s.st_size;

	    char* f = mmap (NULL, size, PROT_READ, MAP_PRIVATE, fd, 0);
	    char** registros = string_split(f, "\n");
	    int j = 0;
	    while (registros[j] != NULL) {
			char** datos_registro = string_split(registros[j], ";");
			if(atoi(datos_registro[1]) == keyBuscada){
				if (atol(datos_registro[0]) > registro->timeStamp) {
					registro->timeStamp = atol(datos_registro[0]);
					registro->key = atoi(datos_registro[1]);
					registro->value = malloc(strlen(datos_registro[2])+1);
					strcpy(registro->value, datos_registro[2]);
				}
			}
			j++;

	    }
		close(fd);

		free(ruta_a_bloque);
		i++;
	}




	free(mi_ruta);


	return registro;
}

