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
#include <commons/collections/list.h>
#include <time.h>

t_log* logger;
t_log* tiempos_de_compactacion;
t_list* mem_table;
int max_value_size;
int tiempo_dump;
pthread_mutex_t mem_table_mutex;
pthread_mutex_t bitarray_mutex;
pthread_mutex_t metadatas_tablas_mutex;
t_list* metadatas_tablas;
t_dictionary* bloqueo_tablas;
pthread_mutex_t bloqueo_tablas_mutex;


char* ruta;
void *receptorDeConsultas(void *);
int lfs_blocks;
int lfs_block_size;
int cantidad_de_dumpeos = 1;
t_bitarray* bitmap;
t_config* config;

int main() {
	logger = iniciar_logger();
	tiempos_de_compactacion = log_create("tiempos_bloqueo.log", "LFS", 0, LOG_LEVEL_INFO);
	char* config_path;
	printf("Ingrese ruta del archivo de configuración de LFS \n");
	char* entrada = leerConsola();
	config_path = malloc(strlen(entrada));
	strcpy(config_path, entrada);
	free(entrada);

	config = config_create(config_path);

	ruta = config_get_string_value(config, "PUNTO_MONTAJE");
	if (!existe_filesystem(ruta)) {
		montar_filesystem();
	}
	pthread_mutex_init(&mem_table_mutex, NULL);
	pthread_mutex_init(&bitarray_mutex, NULL);
	pthread_mutex_init(&metadatas_tablas_mutex, NULL);
	pthread_mutex_init(&bloqueo_tablas_mutex, NULL);

	bloqueo_tablas = dictionary_create();
	mem_table = list_create();
	logger = iniciar_logger();
	//t_config* config = leer_config();

	char* puerto = config_get_string_value(config, "PUERTO_ESCUCHA");
	ruta = config_get_string_value(config, "PUNTO_MONTAJE");
	log_debug(logger, puerto);
	max_value_size = config_get_int_value(config, "TAMANIO_VALUE");
	tiempo_dump = config_get_int_value(config, "TIEMPO_DUMP");

	char* path_to_metadata = string_new();
	string_append(&path_to_metadata, ruta);
	string_append(&path_to_metadata, "/Metadata/Metadata.bin");
	log_debug(logger, path_to_metadata);

	t_config* metadata_lfs = config_create(path_to_metadata);
	lfs_blocks = config_get_int_value(metadata_lfs, "BLOCKS");
	lfs_block_size = config_get_int_value(metadata_lfs, "BLOCK_SIZE");

	pthread_t threadConsola;
	int retorno_de_consola;

	//escribir_bitarray(ruta);
	levantar_bitmap(ruta);

	log_debug(logger, "Levante el Bitmap");

	log_debug(logger, "Primer bloque libre: %i", primer_bloque_libre_sin_set());

	retorno_de_consola = pthread_create(&threadConsola, NULL,
			recibir_por_consola, NULL);
	if (retorno_de_consola) {
		fprintf(stderr, "Error - pthread_create() return code: %d\n",
				retorno_de_consola);
		exit(EXIT_FAILURE);
	}
	pthread_detach(threadConsola);


	pthread_t threadDumpeo;
	int status_thread_dumpeo;

	status_thread_dumpeo = pthread_create(&threadDumpeo, NULL, dump, NULL);
	if (status_thread_dumpeo) {
		fprintf(stderr, "Error - pthread_create() return code: %d\n", status_thread_dumpeo);
		exit(EXIT_FAILURE);
	}

	metadatas_tablas = lfs_describe(ruta);
	if (metadatas_tablas) {
		pthread_t threadCompactacion;
		int status_thread_compactacion;

		for (int i = 0; i < metadatas_tablas->elements_count; i++) {
			Metadata* una_tabla = list_get(metadatas_tablas, i);
			crear_hilo_compactacion_de_tabla(una_tabla);
			crear_mutex_de_tabla(una_tabla);
		}
	} else {
		log_debug(logger, "No hay tablas todavia!");
		metadatas_tablas = list_create();
	}




	struct addrinfo hints;
	struct addrinfo *serverInfo;

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

	log_info(logger, "Esperando memoria...");

	struct sockaddr_in addr; // Esta estructura contendra los datos de la conexion del cliente. IP, puerto, etc.
	socklen_t addrlen = sizeof(addr);

	int socketCliente = accept(listenningSocket, (struct sockaddr *) &addr,
			&addrlen);

	if (!recibir_handshake(MEMORY, socketCliente)) {
		log_info(logger, "Handshake invalido \n");
		return 0;
	}
	send(socketCliente, &max_value_size, sizeof(u_int16_t), 0);

	//thread receptor
	pthread_t threadL;
	int iret1;

	iret1 = pthread_create(&threadL, NULL, receptorDeConsultas,
			(void*) socketCliente);
	if (iret1) {
		fprintf(stderr, "Error - pthread_create() return code: %d\n", iret1);
		exit(EXIT_FAILURE);
	}
	//thread receptor


	int socketNuevo;
	while (true) {
		socketNuevo = accept(listenningSocket, (struct sockaddr *) &addr,
				&addrlen);
		log_debug(logger, "Acepte una conexion");

		log_debug(logger, "A ver si es una memoria");

		if (!recibir_handshake(MEMORY, socketNuevo)) {
			log_debug(logger, "Acepte una conexion");

			log_info(logger, "Handshake invalido \n");
			return 0;
		}
		log_debug(logger, "Efectivamente Rick, es una memoria");

		send(socketNuevo, &max_value_size, sizeof(u_int16_t), 0);
		log_debug(logger, "Le mande el max_value_size");

		//thread receptor
		pthread_t threadL;
		int iret1;

		iret1 = pthread_create(&threadL, NULL, receptorDeConsultas,
				(void*) socketNuevo);
		if (iret1) {
			fprintf(stderr, "Error - pthread_create() return code: %d\n",
					iret1);
			exit(EXIT_FAILURE);
		}
		log_debug(logger, "Cree el hilo perf");

		//thread receptor
	}

	//receptorDeConsultas(socketCliente);

	close(listenningSocket);

	log_destroy(logger);
	config_destroy(config);

	return 0;
}

t_config* leer_config() {
	return config_create(CONFIG_PATH);
}

t_log* iniciar_logger(void) {
	char* lfs = "LFS";
	return log_create(LOG_FILE_PATH, lfs, 1, LOG_LEVEL_DEBUG);
}



void crear_hilo_compactacion_de_tabla(Metadata* una_tabla) {
	pthread_t threadCompactacion;
	int status_thread_compactacion;
	status_thread_compactacion = pthread_create(&threadCompactacion, NULL, compactar_tabla, una_tabla);
	if (status_thread_compactacion) {
		fprintf(stderr, "Error - pthread_create() return code: %d\n", status_thread_compactacion);
		exit(EXIT_FAILURE);
	}
	pthread_detach(threadCompactacion);
}

void crear_mutex_de_tabla(Metadata* una_tabla) {
	pthread_mutex_t* mutex_tabla = malloc(sizeof(pthread_mutex_t));
	pthread_mutex_init(mutex_tabla, NULL);

	pthread_mutex_lock(&bloqueo_tablas_mutex);
	dictionary_put(bloqueo_tablas, una_tabla->nombre_tabla, mutex_tabla);
	pthread_mutex_unlock(&bloqueo_tablas_mutex);
}

void lock_mutex_tabla(char* nombre_tabla) {
	pthread_mutex_lock(&bloqueo_tablas_mutex);
	pthread_mutex_t* mutex_tabla = dictionary_get(bloqueo_tablas, nombre_tabla);
	log_debug(logger, "La tabla esta bloqueada");
	pthread_mutex_lock(mutex_tabla);
	pthread_mutex_unlock(&bloqueo_tablas_mutex);
}

void unlock_mutex_tabla(char* nombre_tabla) {
	pthread_mutex_lock(&bloqueo_tablas_mutex);
	pthread_mutex_t* mutex_tabla = dictionary_get(bloqueo_tablas, nombre_tabla);
	pthread_mutex_unlock(mutex_tabla);
	log_debug(logger, "La tabla se desbloqueo");
	pthread_mutex_unlock(&bloqueo_tablas_mutex);
}

void* ejecutar_comando(int header, void* package) {
	log_debug(logger, "Ejecutando");
	switch (header) {
	case SELECT:
		return lfs_select((t_PackageSelect*) package);
		break;
	case INSERT:
		return lfs_insert((t_PackageInsert*) package);
		break;
	case CREATE:
		return lfs_create((t_PackageCreate*) package);
		break;
	case DESCRIBE:
		if (((t_PackageDescribe*) package)->tabla_long == 1) {
			return lfs_describe(ruta);
		} else {
			return lfs_describe_a_table(ruta,
					((t_PackageDescribe*) package)->nombre_tabla);
		}
		break;
	case DROP:
		return lfs_drop(((t_PackageDrop*) package)->nombre_tabla);
	}
}

Registro* lfs_select(t_PackageSelect* package) {

	char* mi_ruta = string_new();
	string_append(&mi_ruta, ruta);

	log_debug(logger, mi_ruta);

	char* tables = "/Tables/";
	string_append(&mi_ruta, tables);
	string_append(&mi_ruta, package->tabla);

	log_debug(logger, mi_ruta);

	if (!existe_tabla(mi_ruta)) {
		log_debug(logger, "No existe la tabla");
		Registro* registro = malloc(sizeof(Registro));
		registro->value = NULL;
		return registro;
	}
	log_debug(logger, "Existe tabla, BRO!");

	Metadata* metadata = obtener_metadata(mi_ruta);
	strcpy(metadata->nombre_tabla,package->tabla);
	loguear_metadata(metadata);

	int particionObjetivo = calcular_particion(package->key,
			metadata->partitions);
	char* particionObjetivo_string = string_itoa(particionObjetivo);
	log_debug(logger, particionObjetivo_string);
	free(particionObjetivo_string);

	lock_mutex_tabla(package->tabla);
	Registro* registro_mayor = encontrar_keys(package->key, particionObjetivo,
			mi_ruta, package->tabla);
	unlock_mutex_tabla(package->tabla);

	free(metadata);
	free(mi_ruta);

	return registro_mayor;
}

int lfs_insert(t_PackageInsert* package) {
	if (package->value_long > max_value_size) {
		return 0;
	}
	char* mi_ruta = string_new();
	string_append(&mi_ruta, ruta);

	log_debug(logger, mi_ruta);

	char* tables = "/Tables/";
	string_append(&mi_ruta, tables);
	string_append(&mi_ruta, package->tabla);

	log_debug(logger, mi_ruta);

	if (!existe_tabla(mi_ruta)) {
		log_debug(logger, "No existe la tabla");
		return 0;
	}
	log_debug(logger, "Existe tabla, BRO!");

	if (!existe_tabla_en_mem_table(package->tabla)) {
		if (!agregar_tabla_a_mem_table(package->tabla)) {
			return 0;
		}
	}

	log_debug(logger, "Voy a crear el registro");
	Registro* registro_a_insertar = malloc(sizeof(Registro));
	registro_a_insertar->key = package->key;
	registro_a_insertar->timeStamp = package->timestamp;

	char* value = malloc(package->value_long + 1);
	strcpy(value, package->value);
	registro_a_insertar->value = malloc(strlen(value) + 1);
	strcpy(registro_a_insertar->value, value);

	loguear_registro(registro_a_insertar);
	free(mi_ruta);

	return insertar_en_mem_table(registro_a_insertar, package->tabla);
}

int lfs_create(t_PackageCreate* package) {
	log_debug(logger, "Creando");
	//Primero creo el directorio para la tabla
	char* directorio = ruta_a_tabla(package->tabla);
	if (existe_tabla(directorio)) {
		free(directorio);
		log_debug(logger, "Existe la tabla papi: %s", package->tabla);
		return 0;
	}

	if (mkdir(directorio, 0700)) {
		free(directorio);
		return 0;
	}


	//Creo el archivo metadata asociado a la tabla

	if (!crear_metadata(package, directorio)) {
		free(directorio);
		return 0;
	}

	//Creo los archivos binarios
	if (!crear_particiones(package->partitions, directorio)) {
		free(directorio);
		return 0;
	}
	free(directorio);

	Metadata* metadata = malloc(sizeof(Metadata));
	metadata->compaction_time = package->compaction_time;
	metadata->consistency = package->consistency;
	metadata->partitions = package->partitions;
	strcpy(metadata->nombre_tabla, package->tabla);

	pthread_mutex_lock(&metadatas_tablas_mutex);
	list_add(metadatas_tablas, metadata);
	pthread_mutex_unlock(&metadatas_tablas_mutex);

	crear_hilo_compactacion_de_tabla(metadata);
	crear_mutex_de_tabla(metadata);

	log_debug(logger, "Cree las particiones");
	return 1;
}

char* ruta_a_tabla(char* tabla) {
	char* mi_ruta = string_new();
	string_append(&mi_ruta, ruta);
	char* tables = "/Tables/";
	string_append(&mi_ruta, tables);
	string_append(&mi_ruta, tabla);
	return mi_ruta;
}

int crear_particiones(int particiones, char* tabla_path) {
	for (int i = 1; i <= particiones; i++) {
		if (!crear_particion(i, tabla_path)) {
			return 0;
		}
	}
	return 1;
}

int primer_bloque_libre() {
	int bloque = 0;
	for (bloque; bloque < lfs_blocks; bloque++) {
		if (!bitarray_test_bit(bitmap, bloque)) {
			bitarray_set_bit(bitmap, bloque);
			return bloque + 1;
		}
	}
	return -1;
}

int primer_bloque_libre_sin_set() {
	int bloque = 0;
	for (bloque; bloque < lfs_blocks; bloque++) {
		if (!bitarray_test_bit(bitmap, bloque)) {
			//bitarray_set_bit(bitmap, bloque);
			return bloque + 1;
		}
	}
	return -1;
}

int crear_particion(int numero, char* tabla_path) {
	char* mi_particion = string_new();
	string_append(&mi_particion, tabla_path);
	string_append(&mi_particion, "/");
	char* numeroString = string_itoa(numero);
	string_append(&mi_particion, numeroString);
	char* bin = ".bin";
	string_append(&mi_particion, bin);
	log_debug(logger, mi_particion);
	int fd = open(mi_particion, O_RDWR | O_CREAT | O_TRUNC, (mode_t) 0700);

	if (fd == -1) {
		return 0;
	}
	char* particion_a_crear = string_new();

	pthread_mutex_lock(&bitarray_mutex);
	int bloque = primer_bloque_libre();
	pthread_mutex_unlock(&bitarray_mutex);
	if (bloque == -1) {
		return 0;
	}

	string_append_with_format(&particion_a_crear, "SIZE=0\nBLOCKS=[%d]",
			bloque);

	size_t textsize = strlen(particion_a_crear) + 1;
	lseek(fd, textsize - 1, SEEK_SET);
	write(fd, "", 1);
	char *map = mmap(0, textsize, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
	memcpy(map, particion_a_crear, strlen(particion_a_crear));
	msync(map, textsize, MS_SYNC);
	munmap(map, textsize);
	close(fd);
	free(particion_a_crear);
	free(numeroString);
	free(mi_particion);

	return 1;
}

int crear_metadata(t_PackageCreate* package, char* directorio) {
	char* mi_directorio = string_new();
	string_append(&mi_directorio, directorio);
	char* metadata = "/Metadata";
	string_append(&mi_directorio, metadata);
	int fd = open(mi_directorio, O_RDWR | O_CREAT | O_TRUNC, (mode_t) 0700);

	if (fd == -1) {
		return 0;
	}
	char* escribir_metadata = string_new();
	string_append(&escribir_metadata, "CONSISTENCY=");
	char* consistencia = consistency_to_str(package->consistency);
	string_append(&escribir_metadata, consistencia);
	string_append(&escribir_metadata, "\nPARTITIONS=");
	char* particiones = string_itoa(package->partitions);
	string_append(&escribir_metadata, particiones);
	string_append(&escribir_metadata, "\nCOMPACTION_TIME=");
	char* tiempo_compactacion = string_itoa(package->compaction_time);
	string_append(&escribir_metadata, tiempo_compactacion);
	size_t textsize = strlen(escribir_metadata) + 1;
	lseek(fd, textsize - 1, SEEK_SET);
	write(fd, "", 1);
	char *map = mmap(0, textsize, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
	memcpy(map, escribir_metadata, strlen(escribir_metadata));
	msync(map, textsize, MS_SYNC);
	munmap(map, textsize);
	close(fd);
	free(mi_directorio);
	free(particiones);
	free(tiempo_compactacion);
	free(escribir_metadata);
	return 1;
}

int existe_tabla_en_mem_table(char* tabla_a_chequear) {
	int es_tabla(Tabla* tabla) {
		if (strcmp(tabla->nombre_tabla, tabla_a_chequear) == 0) {
			return 1;
		}
		return 0;
	}

	//signal
	pthread_mutex_lock(&mem_table_mutex);
	Tabla* tabla_encontrada = (Tabla*) list_find(mem_table, (int) &es_tabla);
	//wait
	pthread_mutex_unlock(&mem_table_mutex);
	if (tabla_encontrada) {
		log_debug(logger, "Existe la tabla en mem_table");
		return 1;
	}
	log_debug(logger, "No existe la tabla en mem_table");
	return 0;
}

int agregar_tabla_a_mem_table(char* tabla) {
	Tabla* tabla_a_agregar = malloc(sizeof(Tabla));
	strcpy(tabla_a_agregar->nombre_tabla, tabla);
	tabla_a_agregar->registros = list_create();

	int cantidad_anterior;
	//signal
	pthread_mutex_lock(&mem_table_mutex);
	cantidad_anterior = mem_table->elements_count;
	int indice_agregado = list_add(mem_table, tabla_a_agregar);
	//wait
	pthread_mutex_unlock(&mem_table_mutex);

	return indice_agregado + 1 > cantidad_anterior;
}

int insertar_en_mem_table(Registro* registro_a_insertar, char* nombre_tabla) {

	int es_tabla(Tabla* tabla) {
		if (strcmp(tabla->nombre_tabla, nombre_tabla) == 0) {
			return 1;
		}
		return 0;
	}
	int cantidad_anterior;
	//signal
	pthread_mutex_lock(&mem_table_mutex);
	Tabla* tabla = (Tabla*) list_find(mem_table, (int) &es_tabla);
	cantidad_anterior = tabla->registros->elements_count;
	int indice_insercion = list_add(tabla->registros, registro_a_insertar);
	//wait
	pthread_mutex_unlock(&mem_table_mutex);

	pthread_mutex_lock(&mem_table_mutex);
	t_list* mem_table_duplicada = list_duplicate(mem_table);
	pthread_mutex_unlock(&mem_table_mutex);

	list_destroy(mem_table_duplicada);

	return indice_insercion + 1 > cantidad_anterior;
}

t_list* lfs_describe(char* punto_montaje) {

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
			if (strcmp(a_directory->d_name, ".")
					&& strcmp(a_directory->d_name, "..")) {
				char* a_table_path = string_new();
				char* table_name = malloc(strlen(a_directory->d_name) + 1);
				memcpy(table_name, a_directory->d_name,
						strlen(a_directory->d_name));
				table_name[strlen(a_directory->d_name)] = '\0';
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
	free(tablas_path);
	if (!metadatas->elements_count) {
		return NULL;
	}
	return metadatas;
}

t_list* lfs_describe_a_table(char* punto_montaje, char* nombre_tabla) {

	Metadata* metadata = malloc(sizeof(Metadata));
	DIR *tables_directory;
	struct dirent *a_directory;
	char* tabla_path = string_new();
	string_append(&tabla_path, punto_montaje);
	string_append(&tabla_path, "/Tables/");
	log_debug(logger, tabla_path);
	string_append(&tabla_path, nombre_tabla);

	if (!existe_tabla(tabla_path)) {
		return NULL;
	}
	metadata = obtener_metadata(tabla_path);
	strcpy(metadata->nombre_tabla, nombre_tabla);
	free(tabla_path);

	t_list* tablas = list_create();
	list_add(tablas, metadata);

	return tablas;
}

int existe_tabla(char* tabla) {
	int status = 1;
	DIR *dirp;

	dirp = opendir(tabla);
	log_debug(logger, tabla);
	if (dirp == NULL) {
		status = 0;
	}
	closedir(dirp);
	return status;
}

void loguear_metadata(Metadata* metadata) {
	log_debug(logger, metadata->nombre_tabla);
	log_debug(logger,
			"Consistencia: %s, Particiones: %i, Tiempo de Compactacion: %ld",
			consistency_to_str(metadata->consistency), metadata->partitions,
			metadata->compaction_time);
}

void loguear_int(int n) {
	char* n_string = string_itoa(n);
	log_debug(logger, n_string);
	free(n_string);
}

void loguear_registro(Registro* registro) {
	log_debug(logger, "Key: %i, Value: %s, Timestamp: %i", registro->key,
			registro->value, registro->timeStamp);
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
	long tiempoDeCompactacion = config_get_long_value(config_metadata,
			"COMPACTION_TIME");
	metadata->compaction_time = tiempoDeCompactacion;

	char* consistencia = config_get_string_value(config_metadata,
			"CONSISTENCY");
	metadata->consistency = consistency_to_int(consistencia);

	config_destroy(config_metadata);
	free(mi_ruta);
	return metadata;
}

int calcular_particion(int key, int cantidad_particiones) {

	return (key % cantidad_particiones) + 1;
}

Registro* encontrar_keys(int keyBuscada, int particion_objetivo, char* una_ruta_a_tabla, char* nombre_tabla) {
	Registro* registro = buscar_en_mem_table(nombre_tabla, keyBuscada);
	char* mi_ruta = string_new();
	string_append(&mi_ruta, una_ruta_a_tabla);
	char* barra = "/";
	string_append(&mi_ruta, barra);
	string_append_with_format(&mi_ruta, "%i", particion_objetivo);;
	char* bin = ".bin";
	string_append(&mi_ruta, bin);

	t_config* particion = config_create(mi_ruta);

	char** blocks = config_get_array_value(particion, "BLOCKS");
	char* todos_los_registros = string_new();
	char* contenido_particion = contenido_de_los_bloques(nombre_tabla, blocks);
	string_append(&todos_los_registros, contenido_particion);
	string_iterate_lines(blocks, (void*) free);
	config_destroy(particion);
	free(contenido_particion);


	int maximo_dumpeo = 0;
	int cantidad_de_dumpeos_fs = cantidad_de_temporales(nombre_tabla);
	if (cantidad_de_dumpeos_fs) {
		int numero_de_temporal = 1;
		do {
			char* mi_ruta_a_a_tabla = ruta_a_tabla(nombre_tabla);

			char* ruta_a_temporal = string_new();
			string_append(&ruta_a_temporal, mi_ruta_a_a_tabla);
			char* otra_barra = "/";
			string_append(&ruta_a_temporal, otra_barra);
			string_append_with_format(&ruta_a_temporal, "%i", numero_de_temporal);
			char* tmp = ".tmp";
			string_append(&ruta_a_temporal, tmp);

			if (access(ruta_a_temporal, F_OK) != -1) {
				t_config* temporal = config_create(ruta_a_temporal);
				char** temporal_blocks = config_get_array_value(temporal, "BLOCKS");
				char* contenido = contenido_de_los_bloques(nombre_tabla, temporal_blocks);
				string_iterate_lines(temporal_blocks, (void*) free);
				string_append(&todos_los_registros, contenido);
				config_destroy(temporal);
				free(temporal_blocks);
				free(contenido);
			}
			free(mi_ruta_a_a_tabla);
			free(ruta_a_temporal);
			numero_de_temporal++;
		} while (numero_de_temporal <= maximo_dumpeo);
	}

	free(blocks);
	free(mi_ruta);

	log_debug(logger, "Registros: %s", todos_los_registros);

	char** registros = string_split(todos_los_registros, "\n");
	int j = 0;
	while (registros[j] != NULL) {
		log_debug(logger, "Leyendo un registro");

		char** datos_registro = string_split(registros[j], ";");
		if (atoi(datos_registro[1]) == keyBuscada) {
			if (atol(datos_registro[0]) > registro->timeStamp) {
				registro->timeStamp = atol(datos_registro[0]);
				registro->key = atoi(datos_registro[1]);
				registro->value = malloc(strlen(datos_registro[2]) + 1);
				strcpy(registro->value, datos_registro[2]);
			}
		}

		string_iterate_lines(datos_registro, (void*) free);
		log_debug(logger, "Libere los datos del registro");

		free(datos_registro);
		log_debug(logger, "Libere datos_registro");

		j++;
	}
	string_iterate_lines(registros, (void*) free);
	free(registros);
	free(todos_los_registros);

	return registro;
}

int cantidad_de_temporales(char* nombre_tabla) {
	char* mi_ruta = ruta_a_tabla(nombre_tabla);

	DIR *dirp;
	if ((dirp = opendir(mi_ruta)) == NULL) {
		return 0;
	}
	struct dirent* dp;
	char* nombre_archivo;
	int cantidad = 0;
	while((dp = readdir(dirp)) != NULL) {
		nombre_archivo = dp->d_name;

		char* tmp = ".tmp";
		log_warning(logger, nombre_archivo);
		if (string_contains(nombre_archivo, tmp)) {
			char** spliteado = string_split(nombre_archivo, ".");
			char* numero_dumpeo = spliteado[0];
			cantidad++;
			string_iterate_lines(spliteado, (void*) free);
			free(spliteado);
		}
	}
	free(mi_ruta);
	closedir(dirp);
	return cantidad;
}

int cantidad_de_temporales_c(char* nombre_tabla) {
	char* mi_ruta = ruta_a_tabla(nombre_tabla);

	DIR *dirp;
	if ((dirp = opendir(mi_ruta)) == NULL) {
		return 0;
	}
	struct dirent* dp;
	char* nombre_archivo;
	int cantidad = 0;
	while((dp = readdir(dirp)) != NULL) {
		nombre_archivo = dp->d_name;

		char* tmp = ".tmpc";
		log_warning(logger, nombre_archivo);
		if (string_contains(nombre_archivo, tmp)) {
			char** spliteado = string_split(nombre_archivo, ".");
			char* numero_dumpeo = spliteado[0];
			cantidad++;
			string_iterate_lines(spliteado, (void*) free);
			free(spliteado);
		}
	}
	free(mi_ruta);
	closedir(dirp);
	return cantidad;
}

char* contenido_de_los_bloques(char* nombre_tabla, char** blocks) {
	int i = 0;
	char* contenido = string_new();
	char* f;
	while (blocks[i] != NULL) {
		char* ruta_a_bloque = string_new();
		string_append(&ruta_a_bloque, ruta);
		string_append(&ruta_a_bloque, "/Bloques/");
		string_append(&ruta_a_bloque, blocks[i]);
		string_append(&ruta_a_bloque, ".bin");

		log_debug(logger, ruta_a_bloque);

		int fd = open(ruta_a_bloque, O_RDONLY, S_IRUSR | S_IWUSR);

		int size;
		struct stat s;
		int status = fstat(fd, &s);
		size = s.st_size;

		f = mmap(NULL, size, PROT_READ, MAP_PRIVATE, fd, 0);
		if (!size) {
			log_warning(logger, "Bloque vacío");
		} else {
			string_append(&contenido, f);
			munmap(f, size);
		}
		free(ruta_a_bloque);
		i++;
		close(fd);
	}
	return contenido;
}

Registro* buscar_en_mem_table(char* nombre_tabla, int keyBuscada) {
	if (!existe_tabla_en_mem_table(nombre_tabla)) {
		Registro* registro = malloc(sizeof(Registro));
		registro->value = NULL;
		registro->timeStamp = 0;
		return registro;
	}

	int es_tabla(Tabla* tabla) {
		if (strcmp(tabla->nombre_tabla, nombre_tabla) == 0) {
			return 1;
		}
		return 0;
	}
	int es_registro(Registro* registro) {
		return registro->key == keyBuscada;
	}

	Registro* get_mayor_timestamp(Registro* unRegistro, Registro* otroRegistro) {
		return unRegistro->timeStamp >= otroRegistro->timeStamp ?
				unRegistro : otroRegistro;
	}

	Registro* registro_mayor;

	pthread_mutex_lock(&mem_table_mutex);
	Tabla* tabla = (Tabla*) list_find(mem_table, (int) &es_tabla);

	t_list* registros_con_key = list_filter(tabla->registros,
			(void*) es_registro);

	if (registros_con_key->elements_count) {
		Registro* registro_mayor = list_get(registros_con_key, 0);

		registro_mayor = list_fold(registros_con_key, registro_mayor,
				(void*) &get_mayor_timestamp);

		pthread_mutex_unlock(&mem_table_mutex);
		list_destroy(registros_con_key);

		Registro* copia_registro = malloc(sizeof(Registro));
		copia_registro->key = registro_mayor->key;
		copia_registro->timeStamp = registro_mayor->timeStamp;
		copia_registro->value = malloc(strlen(registro_mayor->value) + 1);
		strcpy(copia_registro->value, registro_mayor->value);
		return copia_registro;
	}
	pthread_mutex_unlock(&mem_table_mutex);

	Registro* registro = malloc(sizeof(Registro));
	registro->value = NULL;
	registro->timeStamp = 0;
	return registro;
}

int lfs_drop(char* nombre_tabla){

	char* mi_ruta_a_tabla = ruta_a_tabla(nombre_tabla);

	if(!existe_tabla(mi_ruta_a_tabla)){
		log_info(logger, "No existe la tabla %s", nombre_tabla);
		return 0;
	}

	Metadata* metadata = obtener_metadata(mi_ruta_a_tabla);
	int particiones = metadata->partitions;
	for(int numero_particion = 1; numero_particion <= particiones; numero_particion++){
		char* ruta_a_particion = string_new();
		string_append(&ruta_a_particion, mi_ruta_a_tabla);
		char* barra = "/";
		string_append(&ruta_a_particion, barra);
		string_append_with_format(&ruta_a_particion, "%i", numero_particion);
		char* bin = ".bin";
		string_append(&ruta_a_particion, bin);
		t_config* particion = config_create(ruta_a_particion);
		char** blocks = config_get_array_value(particion, "BLOCKS");


		if(!eliminar_contenido_bloques(blocks)){
			return 0;
		}


		string_iterate_lines(blocks, (void*) free);
		free(blocks);
		config_destroy(particion);
		free(ruta_a_particion);
	}
	free(metadata);

	for(int numero_temporal = 1; numero_temporal <= cantidad_de_dumpeos; numero_temporal++){
		char* ruta_a_temporal = string_new();
		string_append(&ruta_a_temporal, ruta_a_tabla);
		char* barra = "/";
		string_append(&ruta_a_temporal, barra);
		string_append_with_format(&ruta_a_temporal, "%i", numero_temporal);
		char* tmp = ".tmp";
		string_append(&ruta_a_temporal, tmp);
		if (access(ruta_a_temporal, F_OK) == -1){
			free(ruta_a_temporal);
			continue;
		}
		t_config* temporal = config_create(ruta_a_temporal);
		char** blocks = config_get_array_value(temporal, "BLOCKS");

		if(!eliminar_contenido_bloques(blocks)){
			return 0;
		}

		string_iterate_lines(blocks, (void*) free);
		config_destroy(temporal);
		free(blocks);
		free(ruta_a_temporal);
	}

	remove_directory(mi_ruta_a_tabla);
	free(mi_ruta_a_tabla);

	if(existe_tabla_en_mem_table(nombre_tabla)){
		//Hay que extraer en una funcion aparte es_tabla...

		int es_tabla(Tabla* tabla) {
			if (strcmp(tabla->nombre_tabla, nombre_tabla) == 0) {
				return 1;
			}
			return 0;
		}

		pthread_mutex_lock(&mem_table_mutex);
		Tabla* tabla_encontrada = (Tabla*) list_find(mem_table, (int) &es_tabla);

		void registro_destroy(Registro* un_registro) {
		    free(un_registro->value);
		    free(un_registro);
		}

		list_destroy_and_destroy_elements(tabla_encontrada->registros, (void*) registro_destroy);
		list_remove_by_condition(mem_table,(int) &es_tabla);
		pthread_mutex_unlock(&mem_table_mutex);
		log_debug(logger, "Elimine la tabla de mem table");
	}


	return 1;
}


int remove_directory(const char *path) {
	DIR *d = opendir(path);
	size_t path_len = strlen(path);
	int r = -1;

	if (d) {
		struct dirent *p;

		r = 0;

		while (!r && (p = readdir(d))) {
			int r2 = -1;
			char *buf;
			size_t len;

			/* Skip the names "." and ".." as we don't want to recurse on them. */
			if (!strcmp(p->d_name, ".") || !strcmp(p->d_name, "..")) {
				continue;
			}

			len = path_len + strlen(p->d_name) + 2;
			buf = malloc(len);

			if (buf) {
				struct stat statbuf;

				snprintf(buf, len, "%s/%s", path, p->d_name);

				if (!stat(buf, &statbuf)) {
					if (S_ISDIR(statbuf.st_mode)) {
						r2 = remove_directory(buf);
					} else {
						r2 = unlink(buf);
					}
				}

				free(buf);
			}

			r = r2;
		}

		closedir(d);
	}

	if (!r) {
		r = rmdir(path);
	}

	return r;
}

char* ruta_a_bloque(char* numero_bloque) {
	log_debug(logger, "Numero de bloque %s", numero_bloque);
	char* ruta_a_bloque = string_new();
	string_append(&ruta_a_bloque, ruta);
	string_append(&ruta_a_bloque, "/Bloques/");
	string_append(&ruta_a_bloque, numero_bloque);
	string_append(&ruta_a_bloque, ".bin");
	return ruta_a_bloque;
}

int eliminar_contenido_bloques(char** blocks){
	int i = 0;
	while (blocks[i] != NULL) {
		log_debug(logger, "Numero de bloque %s", blocks[i]);
		char* mi_ruta_a_bloque = ruta_a_bloque(blocks[i]);
		log_debug(logger, mi_ruta_a_bloque);

		if (unlink(mi_ruta_a_bloque) == -1) {
			return 0;
		}
		log_debug(logger, "Borre el bloque %s", blocks[i]);

		int fd = open(mi_ruta_a_bloque, O_RDWR | O_CREAT | O_TRUNC, (mode_t) 0700);

		if (fd == -1) {
			return 0;
		}

		log_debug(logger, "Cree el bloque %s", blocks[i]);


		int bloque = atoi(blocks[i]);
		log_debug(logger, "%d", bloque);

		close(fd);
		pthread_mutex_lock(&bitarray_mutex);
		bitarray_clean_bit(bitmap,bloque - 1);
		pthread_mutex_unlock(&bitarray_mutex);
		i++;
		free(mi_ruta_a_bloque);

	}
	return 1;
}

void liberar_registro(Registro* registro) {
	free(registro->value);
	free(registro);
}

void *receptorDeConsultas(void* socket) {

	int socketCliente = (int) socket;

	t_PackagePosta package;
	int status = 1;		// Estructura que maneja el status de los recieve.

	log_info(logger, "Memoria conectada. Esperando Envio de mensajes");

	int headerRecibido;

	while (status) {

		log_info(logger, "Listo para recibir consulta");
		headerRecibido = recieve_header(socketCliente);

		status = headerRecibido;

		if (status) {
			if (headerRecibido == SELECT) {

				log_debug(logger, "Got a SELECT");

				t_PackageSelect package;
				status = recieve_and_deserialize_select(&package,
						socketCliente);

				Registro* registro_a_devolver = (Registro*) ejecutar_comando(
						headerRecibido, &package);

				t_Respuesta_Select respuesta;

				if (registro_a_devolver->value != NULL) {
					log_debug(logger, "hola");
					respuesta.result = 1;
					respuesta.value = malloc(
							strlen(registro_a_devolver->value) + 1);
					strcpy(respuesta.value, registro_a_devolver->value);
					respuesta.value_long = strlen(respuesta.value);
					respuesta.timestamp = registro_a_devolver->timeStamp;
					log_debug(logger, respuesta.value);
				} else {
					log_debug(logger, "Llegue2");
					respuesta.result = 0;
					respuesta.value = malloc(1);
					strcpy(respuesta.value, "");
					respuesta.value_long = 1;
					respuesta.timestamp = 0;
					log_debug(logger, "No esta la key buscada");
				}

				char* serializedPackage = serializarRespuestaSelect(&respuesta);
				send(socketCliente, serializedPackage,
						sizeof(respuesta.result) + sizeof(respuesta.value_long)
								+ respuesta.value_long
								+ sizeof(respuesta.timestamp), 0);
				free(serializedPackage);
				free(respuesta.value);
				free(registro_a_devolver->value);
				free(registro_a_devolver);
				free(package.tabla);
			} else if (headerRecibido == INSERT) {

				log_debug(logger, "Got an INSERT");

				t_PackageInsert package;
				status = recieve_and_deserialize_insert(&package,
						socketCliente);

				int fue_exitoso = (int) ejecutar_comando(headerRecibido,
						&package);
				if (fue_exitoso) {
					log_info(logger, "Se inserto exitosamente");
				} else {
					log_info(logger, "No se pudo insertar");
				}

				free(package.tabla);
				free(package.value);


			} else if (headerRecibido == CREATE) {

				log_debug(logger, "Got a CREATE");

				t_PackageCreate package;
				status = recieve_and_deserialize_create(&package,
						socketCliente);

				int fue_exitoso = (int) ejecutar_comando(headerRecibido,
						&package);
				if (fue_exitoso) {
					log_info(logger, "Se creo exitosamente");
				} else {
					log_info(logger, "No se pudo crear");
				}

				free(package.tabla);

			}else if (headerRecibido == DROP) {
				log_debug(logger, "Got a DROP");

				t_PackageDrop package;
				status = recieve_and_deserialize_drop(&package,
						socketCliente);
				int fue_exitoso = (int) ejecutar_comando(headerRecibido,
						 &package);
				if(fue_exitoso) {
					log_info(logger, "Elimine la tabla %s correctamente", package.nombre_tabla);
				} else {
					log_info(logger, "No pude eliminar la tabla %s ", package.nombre_tabla);
				}

				free(package.nombre_tabla);


			}else if (headerRecibido == DESCRIBE) {

				log_debug(logger, "Got a DESCRIBE");

				t_PackageDescribe package;
				status = recieve_and_deserialize_describe_request(&package,
						socketCliente);

				t_list* metadatas = (t_list*) ejecutar_comando(headerRecibido,
						&package);

				if (metadatas != NULL) {

					t_describe* describe = malloc(sizeof(t_describe));
					int cantidad_de_tablas = metadatas->elements_count;
					describe->cant_tablas = cantidad_de_tablas;
					describe->tablas = malloc(
							cantidad_de_tablas * sizeof(t_metadata));

					for (int i = 0; i < cantidad_de_tablas; i++) {
						Metadata* a_metadata = (Metadata*) list_get(metadatas,
								i);
						t_metadata meta;
						meta.consistencia = a_metadata->consistency;
						strcpy(meta.nombre_tabla, a_metadata->nombre_tabla);

						describe->tablas[i] = meta;
					}

					char* serializedPackage;
					serializedPackage = serializarDescribe(describe);
					char* cantidad_de_tablas_string = string_itoa(
							cantidad_de_tablas);
					log_debug(logger, cantidad_de_tablas_string);
					free(cantidad_de_tablas_string);
					send(socketCliente, serializedPackage,
							cantidad_de_tablas * sizeof(t_metadata)
									+ sizeof(describe->cant_tablas), 0);

					dispose_package(&serializedPackage);

					free(describe->tablas);
					free(describe);
					for (int i = 0; i < metadatas->elements_count; i++) {
						free(list_get(metadatas, i));
					}
					list_destroy(metadatas);

				} else {

					t_describe* describe = malloc(sizeof(t_describe));
					describe->tablas = malloc(sizeof(t_metadata));
					describe->cant_tablas = 1;
					t_metadata meta;
					meta.consistencia = SC;
					strcpy(meta.nombre_tabla, "NO_TABLE");

					describe->tablas[0] = meta;

					char* serializedPackage;
					serializedPackage = serializarDescribe(describe);

					send(socketCliente, serializedPackage,
							sizeof(t_metadata) + sizeof(describe->cant_tablas),
							0);

					dispose_package(&serializedPackage);
					//list_destroy(metadatas);
				}

			}

		}

	}

	log_info(logger, "Cliente Desconectado");

	close(socketCliente);
}

void *recibir_por_consola() {
	char* consulta;
	while (true) {
		consulta = readline("LFS> ");
		if (!consulta) {
			break;
		}
		char* parametros;
		int header;
		int entradaValida = 1;

		separarEntrada(consulta, &header, &parametros);

		if (header == EXIT_CONSOLE) {
			bitarray_destroy(bitmap);
			free(consulta);
			free(parametros);
			rl_clear_history();
			log_error(logger, "Bye");
			return NULL;
		} else if (header == ERROR) {
			log_error(logger, "Comando no valido");
			entradaValida = 0;
		}

		if (entradaValida) {
			//Ejecutar el comando
			interpretarComando(header, parametros);
		}
		add_history(consulta);

		free(consulta);
		free(parametros);

	}
}

void interpretarComando(int header, char* parametros) {

	void* package;
	switch (header) {
	case SELECT:
		package = (t_PackageSelect*) malloc(sizeof(t_PackageSelect));
		if (!fill_package_select(package, parametros)) {
			log_error(logger, "Parametros incorrectos");
			break;
		}
		Registro* registro = lfs_select(package);
		if (registro->value) {
			loguear_registro(registro);
		} else {
			log_error(logger, "No existe un registro con esa key");
		}
		free(registro->value);
		free(registro);
		free(((t_PackageSelect*) package)->tabla);
		free(package);
		break;
	case INSERT:
		package = (t_PackageInsert*) malloc(sizeof(t_PackageInsert));
		log_warning(logger, parametros);
		if (!fill_package_insert(package, parametros, 1)) {
			log_error(logger, "Parametros incorrectos");
			break;
		}
		if (lfs_insert(package)) {
			log_info(logger, "Se inserto exitosamente");
			free(((t_PackageInsert*) package)->tabla);
			free(((t_PackageInsert*) package)->value);
			free(package);
			break;
		}
		log_info(logger, "No se pudo insertar");
		free(((t_PackageInsert*) package)->tabla);
		free(((t_PackageInsert*) package)->value);
		free(package);
		break;
	case DESCRIBE:
		package = (t_PackageDescribe*) malloc(sizeof(t_PackageDescribe));
		if (parametros == NULL) {
			log_warning(logger, "No vino el nombre de la tabla papi");
		}

		if (!fill_package_describe(package, parametros)) {
			log_error(logger, "Parametros incorrectos");
			break;
		}

		if (((t_PackageDescribe*) package)->nombre_tabla) {
			Metadata* metadata;
			t_list* lista = lfs_describe_a_table(ruta, ((t_PackageDescribe*) package)->nombre_tabla);
			if (lista != NULL) {
				metadata = list_get(lista, 0);
				loguear_metadata(metadata);
				free(metadata);
			}
			if (lista == NULL) {
				log_info(logger, "No se hallo la metadata de la tabla: %s",
						((t_PackageDescribe*) package)->nombre_tabla);
				break;
			}
			free(((t_PackageDescribe*) package)->nombre_tabla);
			free(package);
			break;
		}
		t_list* metadatas = lfs_describe(ruta);
		list_iterate(metadatas, (void*) loguear_metadata);

		list_destroy_and_destroy_elements(metadatas, (void*) free);
		free(((t_PackageDescribe*) package)->nombre_tabla);
		free(package);
		break;
	case DROP:
		package = (t_PackageDrop*) malloc(sizeof(t_PackageDrop));

		if(!fill_package_drop(package, parametros)){
			log_error(logger, "Parametros incorrectos");
			break;
		}
		if(lfs_drop(((t_PackageDrop*) package)->nombre_tabla)) {
			log_info(logger, "Se elimino la tabla correctamente");
			break;
		}
		log_info(logger, "No se pudo eliminar la tabla");
		free(((t_PackageDrop*) package)->nombre_tabla);
		free(package);
		break;
	case CREATE:
		package = (t_PackageCreate*) malloc(sizeof(t_PackageCreate));
		if (!fill_package_create(package, parametros)) {
			log_error(logger, "Parametros incorrectos");
			break;
		}
		if (lfs_create(package)) {
			log_info(logger, "Se creo la tabla correctamente");
			break;
		}
		log_info(logger, "No se pudo crear la tabla");
		free(((t_PackageCreate*) package)->tabla);
		free(package);
		break;
	case -1:
		break;
	}

}

int levantar_bitmap(char* punto_montaje) {
	char* ruta_a_bitmap = string_new();
	string_append(&ruta_a_bitmap, punto_montaje);

	char* bitmap_path = "/Metadata/Bitmap.bin";
	string_append(&ruta_a_bitmap, bitmap_path);

	int fd = open(ruta_a_bitmap, O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);

	if (fd == -1) {
		log_error(logger, "No se pudo abrir el Bitmap");
		return 0;
	}

	char* bitmap_char = mmap(NULL, lfs_blocks / 8, PROT_READ | PROT_WRITE,
	MAP_SHARED, fd, 0);

	bitmap = bitarray_create_with_mode(bitmap_char, lfs_blocks / 8, LSB_FIRST);

	//munmap(ruta_a_bitmap, lfs_blocks/8);
	close(fd);

	free(ruta_a_bitmap);
	log_warning(logger, "Bits que maneja el bitarray: %i",
			bitarray_get_max_bit(bitmap));

	return 1;
}

int escribir_bitarray(char* punto_montaje) {
	char* ruta_a_bitmap = string_new();
	string_append(&ruta_a_bitmap, punto_montaje);

	string_append(&ruta_a_bitmap, "/Metadata/Bitmap.bin");
	printf("%s\n", ruta_a_bitmap);
	char bitmap_char[lfs_blocks / 8];
	for (int i = 0; i < (lfs_blocks / 8); i++) {
		bitmap_char[i] = 0;
	}

	int fd = open(ruta_a_bitmap, O_RDWR | O_CREAT | O_TRUNC, (mode_t) 0700);

	if (fd == -1) {
		log_error(logger, "No se pudo abrir el Bitmap");
		return 0;
	}
	log_debug(logger, "Cantidad de Bloques: %i", lfs_blocks);
	log_debug(logger, "Bytes necesarios: %i", lfs_blocks / 8);

	int written_bytes = write(fd, bitmap_char, lfs_blocks / 8);

	log_debug(logger, "Bytes que escribi: %i", written_bytes);

	close(fd);

	return 0;
}

void* compactar_tabla(Metadata* una_tabla) {

	while (1) {

		usleep(una_tabla->compaction_time * 1000);
		if (!hay_dumpeos(una_tabla->nombre_tabla)) {
			log_debug(logger, "No hay dumpeos de %s, voy a esperar un ratito", una_tabla->nombre_tabla);
			compactar_tabla(una_tabla);
		}
		log_debug(logger, "Hay dumpeos de %s", una_tabla->nombre_tabla);

		double tiempo_bloqueado = 0;

		char* contenido_de_tmpcs = contenido_de_temporales(una_tabla->nombre_tabla, &tiempo_bloqueado);
		cantidad_de_dumpeos = 1;
		log_debug(logger, "Voy a obtener diferencias");
		t_list* diferencia = obtener_diferencias(una_tabla->nombre_tabla, una_tabla->partitions, contenido_de_tmpcs);

		list_iterate(diferencia, (void*) loguear_registro);
		modificar_bloques(una_tabla->nombre_tabla, diferencia, &tiempo_bloqueado);
		list_destroy_and_destroy_elements(diferencia, (void*) liberar_registro);

		log_warning(logger, "La tabla %s se bloqueo por %f", una_tabla->nombre_tabla, tiempo_bloqueado);
		log_info(tiempos_de_compactacion, "La tabla %s se bloqueo por %f", una_tabla->nombre_tabla, tiempo_bloqueado);
	}
}

int hay_dumpeos(char* nombre_tabla) {
	if (cantidad_de_dumpeos == 1) {
		return 0;
	}
	int cantidad_de_temporales_fs = cantidad_de_temporales(nombre_tabla);
	return cantidad_de_temporales_fs;
}

char* contenido_de_temporales(char* nombre_tabla, double* tiempo_bloqueado) {
	if (cantidad_de_dumpeos == 1) {
		log_debug(logger, "No hubo dumpeos");
		return NULL;
	}
	char* contenido = string_new();
	int cantidad_de_temporales_fs = cantidad_de_temporales(nombre_tabla);
	log_debug(logger, "Hay %i temporales a  compactar", cantidad_de_temporales_fs);
	if (cantidad_de_temporales_fs) {
		int numero_de_temporal = 1;
		do {
			char* mi_ruta_a_a_tabla = ruta_a_tabla(nombre_tabla);

			char* ruta_a_temporal = string_new();
			string_append(&ruta_a_temporal, mi_ruta_a_a_tabla);
			char* otra_barra = "/";
			string_append(&ruta_a_temporal, otra_barra);
			string_append_with_format(&ruta_a_temporal, "%i", numero_de_temporal);
			char* tmp = ".tmp";
			string_append(&ruta_a_temporal, tmp);

			char* nueva_ruta_a_temporal = string_new();
			string_append(&nueva_ruta_a_temporal, ruta_a_temporal);
			string_append(&nueva_ruta_a_temporal, "c");

			log_debug(logger, nueva_ruta_a_temporal);
			if (access(ruta_a_temporal, F_OK) != -1) {
				clock_t t;
				lock_mutex_tabla(nombre_tabla);
				t = clock();

				rename(ruta_a_temporal, nueva_ruta_a_temporal);
				unlock_mutex_tabla(nombre_tabla);
				t = clock() - t;
				*tiempo_bloqueado += ((double) t) / CLOCKS_PER_SEC;

				t_config* temporal = config_create(nueva_ruta_a_temporal);
				char** blocks = config_get_array_value(temporal, "BLOCKS");

				char* contenido_de_bloques = contenido_de_los_bloques(nombre_tabla, blocks);
				string_append(&contenido, contenido_de_bloques);

				config_destroy(temporal);

				string_iterate_lines(blocks, (void*) free);
				free(blocks);
				free(contenido_de_bloques);
				log_debug(logger, "Le cambie el nombre a %i.tmp", numero_de_temporal);
			}
			free(mi_ruta_a_a_tabla);
			free(nueva_ruta_a_temporal);
			free(ruta_a_temporal);
			log_debug(logger, "Libere rutas");
			numero_de_temporal++;
		} while (numero_de_temporal <= cantidad_de_temporales_fs);
	}
	return contenido;
}

t_list* obtener_lista_de_registros(char** registros) {
	t_list* lista_registros_actuales = list_create();
	int j = 0;
	while (registros[j] != NULL) {
		log_debug(logger, "Leyendo un registro");
		char** datos_registro = string_split(registros[j], ";");
		Registro* registro = malloc(sizeof(Registro));
		registro->timeStamp = atol(datos_registro[0]);
		registro->key = atoi(datos_registro[1]);
		registro->value = malloc(strlen(datos_registro[2]) + 1);
		strcpy(registro->value, datos_registro[2]);
		list_add(lista_registros_actuales, registro);
		string_iterate_lines(datos_registro, (void*) free);
		log_debug(logger, "Libere los datos del registro");
		free(datos_registro);
		log_debug(logger, "Libere datos_registro");
		j++;
	}
	return lista_registros_actuales;
}


t_list* obtener_diferencias(char* nombre_tabla, int particiones, char* contenido_temporal) {
	char* mi_ruta_a_tabla = ruta_a_tabla(nombre_tabla);
	char* todos_los_registros = string_new();
	for (int i = 1; i <= particiones; i++) {
		char* mi_ruta_a_particion = string_new();
		string_append(&mi_ruta_a_particion, mi_ruta_a_tabla);
		string_append_with_format(&mi_ruta_a_particion, "/%i", i);;
		char* bin = ".bin";
		string_append(&mi_ruta_a_particion, bin);

		log_debug(logger, mi_ruta_a_particion);

		t_config* particion = config_create(mi_ruta_a_particion);

		char** blocks = config_get_array_value(particion, "BLOCKS");
		char* contenido_particion = contenido_de_los_bloques(nombre_tabla, blocks);
		log_debug(logger, "Contenido de particion: %s", contenido_particion);
		string_append(&todos_los_registros, contenido_particion);
		string_iterate_lines(blocks, (void*) free);
		free(blocks);
		config_destroy(particion);
		free(contenido_particion);
		free(mi_ruta_a_particion);
		log_debug(logger, "Todos los registros: %s", todos_los_registros);
	}

	char** registros = string_split(todos_los_registros, "\n");
	t_list* registros_actuales = obtener_lista_de_registros(registros);
	string_iterate_lines(registros, (void*) free);
	free(registros);
	free(todos_los_registros);

	char** mis_registros_temporales =  string_split(contenido_temporal, "\n");
	t_list* registros_temporales = obtener_lista_de_registros(mis_registros_temporales);
	string_iterate_lines(mis_registros_temporales, (void*) free);
	free(mis_registros_temporales);
	free(contenido_temporal);
	free(mi_ruta_a_tabla);

	Registro* registro_a_comparar;

	int _es_registro_por_key(Registro* registro) {
		return registro->key == registro_a_comparar->key;
	}

	void _actualizar_registros_actuales(Registro* registro_actual) {
		registro_a_comparar = registro_actual;
		Registro* registro_temporal_con_misma_key = list_find(registros_temporales, (void*) _es_registro_por_key);
		if (registro_temporal_con_misma_key) {
			log_debug(logger, "Hay registro con igual key");
			if (registro_temporal_con_misma_key->timeStamp >= registro_actual->timeStamp) {
				log_debug(logger, "Voy a actualizar el registro");
				Registro* actual = list_find(registros_actuales, (void*) _es_registro_por_key);
				actual->timeStamp = registro_temporal_con_misma_key->timeStamp;
				free(actual->value);
				actual->value = malloc(strlen(registro_temporal_con_misma_key->value) + 1);
				strcpy(actual->value, registro_temporal_con_misma_key->value);
			}
		}
	}

	void _agregar_temporal_si_no_existe(Registro* registro) {
		registro_a_comparar = registro;
		if (!list_find(registros_actuales, (void*) &_es_registro_por_key)) {
			Registro* registro_duplicado = malloc(sizeof(Registro));
			registro_duplicado->key = registro->key;
			registro_duplicado->timeStamp = registro->timeStamp;
			registro_duplicado->value = malloc(strlen(registro->value) + 1);
			strcpy(registro_duplicado->value, registro->value);
			list_add(registros_actuales, registro_duplicado);
		}
	}


	list_iterate(registros_actuales, (void*) _actualizar_registros_actuales);
	list_iterate(registros_temporales, (void*) _agregar_temporal_si_no_existe);
	list_destroy_and_destroy_elements(registros_temporales, (void*) liberar_registro);
	return registros_actuales;
}


void modificar_bloques(char* nombre_tabla, t_list* registros_a_guardar, double* tiempo_bloqueado) {
	liberar_bloques_de_particion(nombre_tabla, tiempo_bloqueado);
	log_debug(logger, "Libere todos los bloques de particion");
	liberar_bloques_de_temporales(nombre_tabla, tiempo_bloqueado);
	log_debug(logger, "Libere todos los bloques de temporales");
	escribir_registros_en_bloques_nuevos(nombre_tabla, registros_a_guardar, tiempo_bloqueado);
}

void liberar_bloques_de_particion(char* nombre_tabla, double* tiempo_bloqueado) {

	char* mi_ruta_a_tabla = ruta_a_tabla(nombre_tabla);
	Metadata* tabla = obtener_metadata(mi_ruta_a_tabla);
	int particiones = tabla->partitions;
	free(tabla);
	for (int i = 1; i <= particiones; i++) {
		char* mi_ruta_a_particion = string_new();
		string_append(&mi_ruta_a_particion, mi_ruta_a_tabla);
		string_append_with_format(&mi_ruta_a_particion, "/%i", i);;
		char* bin = ".bin";
		string_append(&mi_ruta_a_particion, bin);

		log_debug(logger, "Ruta a particion: %s", mi_ruta_a_particion);

		clock_t t;
		lock_mutex_tabla(nombre_tabla);
		t = clock();
		t_config* particion = config_create(mi_ruta_a_particion);

		char** blocks = config_get_array_value(particion, "BLOCKS");
		eliminar_contenido_bloques(blocks);

		config_set_value(particion, "BLOCKS", "[]");
		config_save(particion);
		config_destroy(particion);
		unlock_mutex_tabla(nombre_tabla);
		t = clock() - t;
		*tiempo_bloqueado += ((double) t) / CLOCKS_PER_SEC;

		free(mi_ruta_a_particion);
		string_iterate_lines(blocks, (void*) free);
		free(blocks);
	}
	free(mi_ruta_a_tabla);
}

void liberar_bloques_de_temporales(char* nombre_tabla, double* tiempo_bloqueado) {
	int cantidad_de_temporales_c_fs = cantidad_de_temporales_c(nombre_tabla);
	if (cantidad_de_temporales_c_fs) {
		int numero_de_temporal = 1;
		do {
			char* mi_ruta_a_a_tabla = ruta_a_tabla(nombre_tabla);

			char* ruta_a_temporal = string_new();
			string_append(&ruta_a_temporal, mi_ruta_a_a_tabla);
			char* otra_barra = "/";
			string_append(&ruta_a_temporal, otra_barra);
			string_append_with_format(&ruta_a_temporal, "%i", numero_de_temporal);
			char* tmpc = ".tmpc";
			string_append(&ruta_a_temporal, tmpc);

			if (access(ruta_a_temporal, F_OK) != -1) {
				clock_t t;
				t = clock();
				lock_mutex_tabla(nombre_tabla);
				t_config* temporal = config_create(ruta_a_temporal);

				char** blocks = config_get_array_value(temporal, "BLOCKS");
				eliminar_contenido_bloques(blocks);
				config_destroy(temporal);
				string_iterate_lines(blocks, (void*) free);
				free(blocks);
				unlink(ruta_a_temporal);
				unlock_mutex_tabla(nombre_tabla);
				t = clock() - t;
				*tiempo_bloqueado += ((double) t) / CLOCKS_PER_SEC;

			}
			free(mi_ruta_a_a_tabla);
			free(ruta_a_temporal);
			numero_de_temporal++;
		} while (numero_de_temporal <= cantidad_de_temporales_c_fs);
	}
}


void escribir_registros_en_bloques_nuevos(char* nombre_tabla, t_list* registros_a_guardar, double* tiempo_bloqueo) {

	char* mi_ruta_a_tabla = ruta_a_tabla(nombre_tabla);
	Metadata* tabla = obtener_metadata(mi_ruta_a_tabla);
	int particiones = tabla->partitions;
	free(tabla);
	free(mi_ruta_a_tabla);

	int particion_actual = 1;

	int _es_de_la_particion(Registro* registro) {
		return calcular_particion(registro->key, particiones) == particion_actual;
	}


	for (int i = 1; i <= particiones; i++) {
		particion_actual = i;
		t_list* registros_de_particion = list_filter(registros_a_guardar, (void*) _es_de_la_particion);

		escribir_registros_de_particion(nombre_tabla, particion_actual, registros_de_particion, tiempo_bloqueo);
		list_destroy(registros_de_particion);
	}


}

void* dump() {

	void _crear_archivo_temporal(char* nombre_tabla, int bytes_a_dumpear) {

		char* temporal_path = ruta_a_tabla(nombre_tabla);

		string_append(&temporal_path, "/");
		string_append_with_format(&temporal_path, "%i", cantidad_de_dumpeos);

		char* tmp = ".tmp";
		string_append(&temporal_path, tmp);
		log_debug(logger, temporal_path);
		int fd = open(temporal_path, O_RDWR | O_CREAT | O_TRUNC, (mode_t) 0700);
		if (fd == -1) {
			return;
		}
		char* temporal_a_crear = string_new();
		string_append_with_format(&temporal_a_crear, "SIZE=%i\nBLOCKS=[",
				bytes_a_dumpear);

		int size_in_fs = 0;
		while (size_in_fs < bytes_a_dumpear) {
			pthread_mutex_lock(&bitarray_mutex);
			int bloque = primer_bloque_libre();
			pthread_mutex_unlock(&bitarray_mutex);

			size_in_fs += lfs_block_size;
			if (!(size_in_fs > bytes_a_dumpear)) {
				string_append_with_format(&temporal_a_crear, "%d,", bloque);
			} else {
				string_append_with_format(&temporal_a_crear, "%d]", bloque);
			}
			log_debug(logger, "Necesite el bloque: %i", bloque);
		}

		size_t textsize = strlen(temporal_a_crear) + 1;
		lseek(fd, textsize - 1, SEEK_SET);
		write(fd, "", 1);
		char *map = mmap(0, textsize, PROT_READ | PROT_WRITE, MAP_SHARED, fd,
				0);
		memcpy(map, temporal_a_crear, strlen(temporal_a_crear));
		msync(map, textsize, MS_SYNC);
		munmap(map, textsize);
		close(fd);
		free(temporal_a_crear);
		free(temporal_path);
		log_debug(logger, "Cree el .tmp papi");
	}

	void _dumpear_tabla(Tabla* tabla) {
		int bytes_a_dumpear = tamanio_de_tabla(tabla);
		lock_mutex_tabla(tabla->nombre_tabla);
		_crear_archivo_temporal(tabla->nombre_tabla, bytes_a_dumpear);
		escribir_registros_en_bloques(tabla);
		unlock_mutex_tabla(tabla->nombre_tabla);
	}

	while (1) {
		log_debug(logger, "Voy a intentar dumpear");
		int nanosegundos_dumpeo = tiempo_dump * 1000;
		usleep(nanosegundos_dumpeo);

		pthread_mutex_lock(&mem_table_mutex);
		if(list_is_empty(mem_table)) {
			log_debug(logger, "No hay nada pa dumpear");
			pthread_mutex_unlock(&mem_table_mutex);
			dump();
		}
		pthread_mutex_unlock(&mem_table_mutex);
		log_debug(logger, "Voy a empezar el dumpeo");
		pthread_mutex_lock(&mem_table_mutex);
		t_list* mem_table_duplicada = list_duplicate(mem_table);
		pthread_mutex_unlock(&mem_table_mutex);

		log_debug(logger, "Voy a dumpear las tablas");
		list_iterate(mem_table_duplicada, (void*) _dumpear_tabla);
		cantidad_de_dumpeos++;
		pthread_mutex_lock(&mem_table_mutex);
		mem_table = list_create();
		pthread_mutex_unlock(&mem_table_mutex);
		//TODO: Destroy elements para liberar el puntero del registro en mem_table
		list_destroy(mem_table_duplicada);
	}
}

int tamanio_de_tabla(Tabla* tabla) {
	log_debug(logger, "Tabla a Dumpear: %s", tabla->nombre_tabla);
	int size = 0;
	Registro* registro;
	log_debug(logger, "Cantidad de Registros: %i",
			tabla->registros->elements_count);
	for (int i = 0; i < tabla->registros->elements_count; i++) {
		registro = list_get(tabla->registros, i);
		size += size_of_Registro(registro);
	}
	log_debug(logger, "Tamanio de tabla: %i", size);
	return size;
}

int size_of_Registro(Registro* registro) {
	int size = 0;
	char* k_string = string_itoa(registro->key);
	size += strlen(k_string);
	free(k_string);
	char* t_string = string_itoa(registro->timeStamp);
	log_debug(logger, t_string);
	size += strlen(t_string);
	free(t_string);
	size += strlen(registro->value);
	size += 3;
	return size;
}

void escribir_registros_de_particion(char* nombre_tabla, int particion, t_list* registros, double* tiempo_bloqueado) {

	Registro* registro;
	char* registro_a_escribir = string_new();

	for (int i = 0; i < registros->elements_count; i++) {
		registro = list_get(registros, i);
		string_append_with_format(&registro_a_escribir, "%ld;", registro->timeStamp);
		string_append_with_format(&registro_a_escribir, "%i;", registro->key);
		string_append_with_format(&registro_a_escribir, "%s\n", registro->value);
		log_debug(logger, "Apeendee un registro");
	}
	log_debug(logger, registro_a_escribir);
	log_debug(logger, "Caracteres a escribir: %i", strlen(registro_a_escribir));
	int indice = 0;

	int size = 0;
	for (int i = 0; i < registros->elements_count; i++) {
		registro = list_get(registros, i);
		size += size_of_Registro(registro);
	}
	if (!size) {
		log_debug(logger, "Nada para la particion %i", particion);
		return;
	}
	t_list* block_list = list_create();
	char* blocks = string_new();
	string_append(&blocks, "[");
	int size_in_fs = 0;
	while (size_in_fs < size) {
		pthread_mutex_lock(&bitarray_mutex);
		int bloque = primer_bloque_libre();
		pthread_mutex_unlock(&bitarray_mutex);

		size_in_fs += lfs_block_size;
		if (!(size_in_fs > size)) {
			string_append_with_format(&blocks, "%d,", bloque);
		} else {
			string_append_with_format(&blocks, "%d]", bloque);
		}
		list_add(block_list, bloque);
		log_debug(logger, "Necesite el bloque: %i", bloque);
	}
	log_debug(logger, "Bloques %s", blocks);

	char** blocks_as_array = string_get_string_as_array(blocks);
	int i = 0;
	while (blocks_as_array[i] != NULL) {
		log_debug(logger, ruta);
		char* ruta_a_bloque = string_new();
		string_append(&ruta_a_bloque, ruta);
		string_append(&ruta_a_bloque, "/Bloques/");
		log_debug(logger, ruta_a_bloque);
		string_append(&ruta_a_bloque, blocks_as_array[i]);
		string_append(&ruta_a_bloque, ".bin");

		log_debug(logger, ruta_a_bloque);

		char* a_escribir_en_bloque = string_new();
		log_debug(logger, "Indice por el que voy: %i", indice);
		while (strlen(a_escribir_en_bloque) < lfs_block_size && indice < strlen(registro_a_escribir)) {

			log_debug(logger, "Agrego este char: %c", registro_a_escribir[indice]);

			string_append_with_format(&a_escribir_en_bloque, "%c",registro_a_escribir[indice]);
			indice++;

			log_debug(logger, a_escribir_en_bloque);
			//log_debug(logger, "Espacio restante en el bloque: %i", lfs_block_size - strlen(a_escribir_en_bloque));
		}
		log_debug(logger, "Voy a abrir el bloque");
		int fd = open(ruta_a_bloque, O_RDWR | O_CREAT | O_TRUNC, (mode_t) 0700);
		log_debug(logger, "Abri fd");

		size_t textsize = strlen(a_escribir_en_bloque) + 1;
		log_debug(logger, "Calcule tamanio");

		lseek(fd, textsize - 1, SEEK_SET);
		log_debug(logger, "Seek");

		write(fd, "", 1);
		char *map = mmap(0, textsize, PROT_READ | PROT_WRITE, MAP_SHARED, fd,
				0);
		log_debug(logger, "Mmapee");

		log_debug(logger, a_escribir_en_bloque);

		memcpy(map, a_escribir_en_bloque, strlen(a_escribir_en_bloque));

		log_debug(logger, "Memcopyie");

		msync(map, textsize, MS_SYNC);
		log_debug(logger, "Msynquie");

		munmap(map, textsize);
		log_debug(logger, "Munmapie");

		close(fd);
		log_debug(logger, "Cerre el fd");

		free(a_escribir_en_bloque);
		log_debug(logger, "Libere a_escribir_en_bloque");

		free(ruta_a_bloque);
		log_debug(logger, "Libere ruta_a_bloque");

		i++;
	}

	free(registro_a_escribir);

	actualizar_bloques_particion(nombre_tabla, particion, block_list, size, tiempo_bloqueado);
	list_destroy(block_list);
	free(blocks);
	string_iterate_lines(blocks_as_array, (void*) free);
	free(blocks_as_array);
}

void actualizar_bloques_particion(char* nombre_tabla, int particion, t_list* blocks, int size, double* tiempo_bloqueado) {

	char* bloques = blocks_to_string(blocks);

	char* mi_ruta_a_particion = ruta_a_tabla(nombre_tabla);;
	string_append_with_format(&mi_ruta_a_particion, "/%i", particion);;
	char* bin = ".bin";
	string_append(&mi_ruta_a_particion, bin);

	log_debug(logger, "Ruta a particion: %s", mi_ruta_a_particion);

	log_debug(logger, bloques);
	clock_t t;
	lock_mutex_tabla(nombre_tabla);
	t = clock();
	t_config* config_particion = config_create(mi_ruta_a_particion);
	config_set_value(config_particion, "BLOCKS", bloques);
	log_debug(logger, "size = %i", size);
	char* string_size = string_itoa(size);
	config_set_value(config_particion, "SIZE", string_size);
	config_save(config_particion);
	config_destroy(config_particion);
	unlock_mutex_tabla(nombre_tabla);
	t = clock() - t;
	*tiempo_bloqueado += ((double) t) / CLOCKS_PER_SEC;

	free(mi_ruta_a_particion);
	free(bloques);
	free(string_size);
}

char* blocks_to_string(t_list* blocks)  {
	char* bloques = string_new();
	string_append(&bloques, "[");

	void _block_to_string(int block) {
		string_append_with_format(&bloques, "%i,", block);
	}
	list_iterate(blocks, (void*)_block_to_string);
    char *bloques_posta = string_substring_until(bloques, strlen(bloques) - 1);
	string_append(&bloques_posta, "]");
	free(bloques);

	return bloques_posta;
}

void escribir_registros_en_bloques(Tabla* tabla) {
	char* temporal_path = ruta_a_tabla(tabla->nombre_tabla);

	string_append(&temporal_path, "/");

	string_append_with_format(&temporal_path, "%i", cantidad_de_dumpeos);

	char* tmp = ".tmp";
	string_append(&temporal_path, tmp);

	log_debug(logger, temporal_path);

	t_config* temporal = config_create(temporal_path);

	log_debug(logger, "Abri la config papi");
	int size = config_get_int_value(temporal, "SIZE");
	char** blocks = config_get_array_value(temporal, "BLOCKS");

	Registro* registro;
	char* registro_a_escribir = string_new();

	for (int i = 0; i < tabla->registros->elements_count; i++) {
		registro = list_get(tabla->registros, i);
		string_append_with_format(&registro_a_escribir, "%ld;",
				registro->timeStamp);
		string_append_with_format(&registro_a_escribir, "%i;", registro->key);
		string_append_with_format(&registro_a_escribir, "%s\n",
				registro->value);
		log_debug(logger, "Apeendee un registro");
	}
	log_debug(logger, registro_a_escribir);
	log_debug(logger, "Caracteres a escribir: %i", strlen(registro_a_escribir));
	int indice = 0;
	int i = 0;
	log_debug(logger, "Numero de bloque: %s", blocks[i]);
	while (blocks[i] != NULL) {
		log_debug(logger, ruta);
		char* ruta_a_bloque = string_new();
		string_append(&ruta_a_bloque, ruta);
		string_append(&ruta_a_bloque, "/Bloques/");
		log_debug(logger, ruta_a_bloque);
		string_append(&ruta_a_bloque, blocks[i]);
		string_append(&ruta_a_bloque, ".bin");

		log_debug(logger, ruta_a_bloque);

		char* a_escribir_en_bloque = string_new();
		log_debug(logger, "Indice por el que voy: %i", indice);
		while (strlen(a_escribir_en_bloque) < lfs_block_size && indice < strlen(registro_a_escribir)) {

			log_debug(logger, "Agrego este char: %c", registro_a_escribir[indice]);

			string_append_with_format(&a_escribir_en_bloque, "%c",registro_a_escribir[indice]);
			indice++;

			log_debug(logger, a_escribir_en_bloque);
			//log_debug(logger, "Espacio restante en el bloque: %i", lfs_block_size - strlen(a_escribir_en_bloque));
		}
		log_debug(logger, "Voy a abrir el bloque");
		int fd = open(ruta_a_bloque, O_RDWR | O_CREAT | O_TRUNC, (mode_t) 0700);
		log_debug(logger, "Abri fd");

		size_t textsize = strlen(a_escribir_en_bloque) + 1;
		log_debug(logger, "Calcule tamanio");

		lseek(fd, textsize - 1, SEEK_SET);
		log_debug(logger, "Seek");

		write(fd, "", 1);
		char *map = mmap(0, textsize, PROT_READ | PROT_WRITE, MAP_SHARED, fd,
				0);
		log_debug(logger, "Mmapee");

		log_debug(logger, a_escribir_en_bloque);

		memcpy(map, a_escribir_en_bloque, strlen(a_escribir_en_bloque));

		log_debug(logger, "Memcopyie");

		msync(map, textsize, MS_SYNC);
		log_debug(logger, "Msynquie");

		munmap(map, textsize);
		log_debug(logger, "Munmapie");

		close(fd);
		log_debug(logger, "Cerre el fd");

		free(a_escribir_en_bloque);
		log_debug(logger, "Libere a_escribir_en_bloque");

		free(ruta_a_bloque);
		log_debug(logger, "Libere ruta_a_bloque");

		i++;
	}
	free(registro_a_escribir);
	config_destroy(temporal);
	string_iterate_lines(blocks, (void*) free);
	free(blocks);
	free(temporal_path);
}

void montar_filesystem() {
	crear_carpeta_en(ruta);
	char* ruta_a_bloques = string_new();
	string_append(&ruta_a_bloques, ruta);
	string_append(&ruta_a_bloques, "/Bloques/");
	crear_carpeta_en(ruta_a_bloques);
	char* ruta_a_metadata = string_new();
	string_append(&ruta_a_metadata, ruta);
	string_append(&ruta_a_metadata, "/Metadata/");
	crear_carpeta_en(ruta_a_metadata);
	char* ruta_a_tables = string_new();
	string_append(&ruta_a_tables, ruta);
	string_append(&ruta_a_tables, "/Tables/");
	crear_carpeta_en(ruta_a_tables);
	free(ruta_a_tables);

	char* metadata_lfs_path;
	printf("Ingrese la ruta metadata del file system \n");
	char* otra_entrada = leerConsola();
	metadata_lfs_path = malloc(strlen(otra_entrada));
	strcpy(metadata_lfs_path, otra_entrada);
	free(otra_entrada);

	printf("%s\n", metadata_lfs_path);

	t_config* metadata_lfs = config_create(metadata_lfs_path);
	lfs_blocks = config_get_int_value(metadata_lfs, "BLOCKS");
	lfs_block_size = config_get_int_value(metadata_lfs, "BLOCK_SIZE");

	printf("%i\n", lfs_blocks);
	printf("%i\n", lfs_block_size);

	crear_n_bloques(lfs_blocks, ruta_a_bloques);

	copiar_metadata_lfs(ruta_a_metadata);

	escribir_bitarray(ruta);

	config_destroy(metadata_lfs);
}

int crear_carpeta_en(char* una_ruta) {
	printf("%s\n", una_ruta);
	if (mkdir(una_ruta, 0700)) {
		return 0;
	}
	return 1;
}

void crear_n_bloques(int n, char* ruta_a_bloques) {
	for (int i = 1; i <= n; i++) {
		char* ruta_a_bloque = string_new();
		string_append(&ruta_a_bloque, ruta_a_bloques);
		string_append_with_format(&ruta_a_bloque, "%i.bin", i);
		printf("%s\n", ruta_a_bloque);
		int fd = open(ruta_a_bloque, O_RDWR | O_CREAT | O_TRUNC, (mode_t) 0700);
		close(fd);
		free(ruta_a_bloque);
	}
}

void copiar_metadata_lfs(char* ruta_a_metadata) {
	char* metadata_path = string_new();
	string_append(&metadata_path, ruta_a_metadata);

	string_append(&metadata_path, "Metadata.bin");

	printf("%s\n", metadata_path);

	int fd = open(metadata_path, O_RDWR | O_CREAT | O_TRUNC, (mode_t) 0700);
	if (fd == -1) {
		return;
	}
	char* metadata_a_crear = string_new();
	string_append_with_format(&metadata_a_crear,
			"BLOCK_SIZE=%i\nBLOCKS=%i\nMAGIC_NUMBER=LISSANDRA", lfs_block_size,
			lfs_blocks);
	size_t textsize = strlen(metadata_a_crear) + 1;
	lseek(fd, textsize - 1, SEEK_SET);
	write(fd, "", 1);
	char *map = mmap(0, textsize, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
	memcpy(map, metadata_a_crear, strlen(metadata_a_crear));
	msync(map, textsize, MS_SYNC);
	munmap(map, textsize);
	close(fd);
	free(metadata_a_crear);
	free(metadata_path);
}

int existe_filesystem(char* punto_montaje) {
	int status = 1;
	DIR *dirp;
	log_debug(logger, punto_montaje);
	dirp = opendir(punto_montaje);
	if (dirp == NULL) {
		status = 0;
	}
	closedir(dirp);
	return status;
}


/*
 int escribir_registro_en_bloque(Registro* registro, char* nombre_tabla) {
	char* temporal_path = ruta_a_tabla(nombre_tabla, ruta);

	string_append(&temporal_path, "/");
	char* cantidad_dumpeos_string = string_itoa(cantidad_de_dumpeos);
	string_append(&temporal_path, cantidad_dumpeos_string);

	char* tmp = ".tmp";
	string_append(&temporal_path, tmp);

	t_config* temporal = config_create(temporal_path);

	int size = config_get_int_value(temporal, "SIZE");
	char** blocks = config_get_array_value(temporal, "BLOCKS");

	int i = 0;
	while (blocks[i] != NULL) {
		char* ruta_a_bloque = string_new();
		string_append(&ruta_a_bloque, ruta);
		string_append(&ruta_a_bloque, "/Bloques/");
		string_append(&ruta_a_bloque, blocks[i]);
		string_append(&ruta_a_bloque, ".bin");

		log_debug(logger, ruta_a_bloque);

		int fd = open(ruta_a_bloque, O_RDONLY, S_IRUSR | S_IWUSR);

		char* registro_a_escribir = string_new();
		string_append_with_format(&registro_a_escribir, "%l;%i;%s",
				registro->timeStamp, registro->key, registro->value);

		log_debug(logger, "Se va a guardar: %s", registro_a_escribir)

		int size_in_fs = 0;
		while (size_in_fs < bytes_a_dumpear) {
			pthread_mutex_lock(&bitarray_mutex);
			int bloque = primer_bloque_libre();
			pthread_mutex_unlock(&bitarray_mutex);

			size_in_fs += lfs_block_size;
			if (!(size_in_fs > bytes_a_dumpear)) {
				string_append_with_format(&temporal_a_crear, "%d,", bloque);
			} else {
				string_append_with_format(&temporal_a_crear, "%d]", bloque);
			}
		}

		size_t textsize = strlen(temporal_a_crear) + 1;
		lseek(fd, textsize - 1, SEEK_SET);
		write(fd, "", 1);
		char *map = mmap(0, textsize, PROT_READ | PROT_WRITE, MAP_SHARED, fd,
				0);
		memcpy(map, temporal_a_crear, strlen(temporal_a_crear));
		msync(map, textsize, MS_SYNC);
		munmap(map, textsize);
		close(fd);
		free(temporal_a_crear);
		free(cantidad_dumpeos_string);
		free(temporal_path);

	}
*/
