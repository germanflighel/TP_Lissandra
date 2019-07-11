/*
 ============================================================================
 Name        : Kernel.c
 Author      : 
 Version     :
 Copyright   : Your copyright notice
 Description : Hello World in C, Ansi-style
 ============================================================================
 */

#include "Kernel.h"
#include <sys/types.h>
#include <unistd.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <stdarg.h>
#include <sys/mman.h>
t_log* logger_Kernel;
int quantum;
int tiempoDescribe;
int multiprocesamiento;
int metadata_refresh;
t_queue* colaReady;
sem_t ejecutar_sem;
pthread_mutex_t cola_ready_mutex;

//sincro
pthread_mutex_t eventual_mutex;
pthread_mutex_t logger_mutex;
pthread_mutex_t tablas_actuales_mutex;
pthread_mutex_t memorias_mutex;
pthread_mutex_t gossiping_mutex;
pthread_mutex_t config_mutex;
//sincro

// metricas

pthread_mutex_t metricas;

int select_totales;
int insert_totales;

Metricas select_sc;
Metricas select_ec;
Metricas select_shc;

Metricas insert_sc;
Metricas insert_ec;
Metricas insert_shc;

// metricas

t_list* exec_mutexes;

t_config *conection_conf;

char* ip_destino;
char* puerto_destino;
struct addrinfo hints;

int strongC = NULL;
t_queue* eventualC;

void *intercambiarTabla();
void *describeNuevo();

int main() {
	/*
	 *
	 *  Estas y otras preguntas existenciales son resueltas getaddrinfo();
	 *
	 *  Obtiene los datos de la direccion de red y lo guarda en serverInfo.
	 *
	 */

	//Inicializacion metricas
	select_sc.consistencia = SC;
	select_sc.tiempoTotal = 0;
	select_sc.cantidad = 0;

	select_ec.consistencia = EC;
	select_ec.tiempoTotal = 0;
	select_ec.cantidad = 0;

	select_shc.consistencia = SHC;
	select_shc.tiempoTotal = 0;
	select_shc.cantidad = 0;

	insert_sc.consistencia = SC;
	select_sc.tiempoTotal = 0;
	select_sc.cantidad = 0;

	insert_ec.consistencia = EC;
	select_ec.tiempoTotal = 0;
	select_ec.cantidad = 0;

	insert_shc.consistencia = SHC;
	select_shc.tiempoTotal = 0;
	select_shc.cantidad = 0;

	//Inicializacion metricas

	logger_Kernel = iniciar_logger();

	memset(&hints, 0, sizeof(hints));
	hints.ai_family = AF_UNSPEC; // Permite que la maquina se encargue de verificar si usamos IPv4 o IPv6
	hints.ai_socktype = SOCK_STREAM;	// Indica que usaremos el protocolo TCP

	abrir_config(&conection_conf);

	char* ip = config_get_string_value(conection_conf, "IP");
	ip_destino = malloc(strlen(ip) + 1);
	strcpy(ip_destino, ip);

	char* puerto = config_get_string_value(conection_conf, "PUERTO_MEMORIA");
	puerto_destino = malloc(strlen(ip) + 1);
	strcpy(puerto_destino, puerto);

	quantum = config_get_int_value(conection_conf, "QUANTUM");
	multiprocesamiento = config_get_int_value(conection_conf,
			"MULTIPROCESAMIENTO");
	metadata_refresh = config_get_int_value(conection_conf, "METADATA_REFRESH");
	tiempoDescribe = 1000 * metadata_refresh;
	multiprocesamiento = config_get_int_value(conection_conf,
			"MULTIPROCESAMIENTO");

	tablaGossiping = list_create();

	pthread_mutex_init(&logger_mutex, NULL);
	pthread_mutex_init(&gossiping_mutex, NULL);
	pthread_mutex_init(&memorias_mutex, NULL);
	pthread_mutex_init(&config_mutex, NULL);
	pthread_mutex_init(&metricas,NULL);

	struct addrinfo *serverInfo;
	getaddrinfo(ip, puerto, &hints, &serverInfo);// Carga en serverInfo los datos de la conexion

	/*
	 * 	Ya se quien y a donde me tengo que conectar... ������Y ahora?
	 *	Tengo que encontrar una forma por la que conectarme al server... Ya se! Un socket!
	 *
	 * 	Obtiene un socket (un file descriptor -todo en linux es un archivo-), utilizando la estructura serverInfo que generamos antes.
	 *
	 */

	int serverSocket;

	/*
	 * 	Perfecto, ya tengo el medio para conectarme (el archivo), y ya se lo pedi al sistema.
	 * 	Ahora me conecto!
	 *
	 */


	Memoria* mem_nueva = malloc(sizeof(Memoria));
	strcpy(mem_nueva->con.puerto, puerto_destino);
	strcpy(mem_nueva->con.ip, ip_destino);
	mem_nueva->socket = malloc(sizeof(int) * multiprocesamiento);
	int num_memoria;
	for (int sock = 0; sock < multiprocesamiento; sock++) {
		serverSocket = socket(serverInfo->ai_family, serverInfo->ai_socktype,
				serverInfo->ai_protocol);
		if (connect(serverSocket, serverInfo->ai_addr, serverInfo->ai_addrlen)
				== 0) {

			log_info_s(logger_Kernel, "conecte");

			enviar_handshake(KERNEL, serverSocket);

			num_memoria = recibir_numero_memoria(serverSocket);
			printf("Num memoria %d\n", num_memoria);

			printf("Conectado al servidor.\n");
			log_info_s(logger_Kernel, "Conecte al servidor.");

			mem_nueva->numero = num_memoria;
			mem_nueva->socket[sock] = serverSocket;

		} else {
			log_info_s(logger_Kernel, "No conecte");
			printf("No se pudo conectar al servidor...");
			log_info_s(logger_Kernel, "No me pude conectar con el servidor");
		}
	}

	freeaddrinfo(serverInfo);	// No lo necesitamos mas

	printf("Esperando describe.\n");


	tablas_actuales = list_create();

	metricas_memorias = list_create();

	crear_metricas(num_memoria);

	memoriasConectadas = list_create();

	exec_mutexes = list_create();

	list_add(memoriasConectadas, mem_nueva);

	//setup planificacion
	colaReady = queue_create();
	sem_init(&ejecutar_sem, 0, 0);
	pthread_mutex_init(&cola_ready_mutex, NULL);
	pthread_mutex_init(&eventual_mutex, NULL);
	pthread_mutex_init(&tablas_actuales_mutex, NULL);

	for (int index = 0; index < multiprocesamiento; index++) {
		pthread_mutex_t* exec_mutex = malloc(sizeof(pthread_mutex_t));
		pthread_mutex_init(exec_mutex, NULL);
		list_add(exec_mutexes, exec_mutex);

		pthread_t threadExec;
		int iret1;

		iret1 = pthread_create(&threadExec, NULL, exec, (void*) index);
		if (iret1) {
			fprintf(stderr, "Error - pthread_create() return code: %d\n",
					iret1);
			exit(EXIT_FAILURE);
		}
	}

	//setup planificacion

	//setup consistencias
	eventualC = queue_create();
	//setup consistencias

	/*
	 //thread executer
	 pthread_t threadL;
	 int iret1;

	 iret1 = pthread_create(&threadL, NULL, exec, (void*) serverSocket);
	 if (iret1) {
	 fprintf(stderr, "Error - pthread_create() return code: %d\n", iret1);
	 exit(EXIT_FAILURE);
	 }
	 //thread executer

	 */
	Script* script_consola = malloc(sizeof(Script));
	script_consola->index = 0;

	script_consola->lineas = string_split("DESCRIBE\n", "\n");
	script_consola->cant_lineas = cant_parametros(script_consola->lineas);

	script_a_ready(script_consola);

	//threadConexiones
	pthread_t threadC;
	int iret2;

	iret2 = pthread_create(&threadC, NULL, intentarEstablecerConexion, NULL);
	if (iret2) {
		fprintf(stderr, "Error - pthread_create() return code: %d\n", iret2);
		exit(EXIT_FAILURE);
	}
	//threadConexiones

	//thread gossiping
	pthread_t threadG;
	int gossipingret;

	gossipingret = pthread_create(&threadG, NULL, intercambiarTabla, NULL);
	if (gossipingret) {
		fprintf(stderr, "Error - pthread_create() return code: %d\n",
				gossipingret);
		exit(EXIT_FAILURE);
	}
	//thread gossiping

	//threadDescribe

	pthread_t threadD;

	int iret3;

	iret3 = pthread_create(&threadD, NULL, describeNuevo, NULL);

	if (iret3) {
		fprintf(stderr, "Error - pthread_create() return code: %d\n", iret3);
		exit(EXIT_FAILURE);
	}

	//threadDescribe

	//thread Inotify
	pthread_t threadInotify;
	int inotify_thread;

	inotify_thread = pthread_create(&threadInotify, NULL, (void*) watch_config,
	CONFIG_PATH);
	if (gossipingret) {
		fprintf(stderr, "Error - pthread_create() return code: %d\n",
				gossipingret);
		exit(EXIT_FAILURE);
	}

	//thread Inotify

	//threadMetrics
	pthread_t threadM;
	int iret4;

	iret4 = pthread_create(&threadM, NULL, (void*) metricsCada30, NULL);

	if (iret4) {
		fprintf(stderr, "Error - pthread_create() return code: %d\n", iret4);
		exit(EXIT_FAILURE);
	}
	//threadMetrics

	int enviar = 1;
	int entradaValida;
	t_PackagePosta package;
	package.message = malloc(MAX_MESSAGE_SIZE);
	char *serializedPackage;

	//t_PackageRec packageRec;
	//int status = 1;		// Estructura que manjea el status de los recieve.

	printf(
			"Bienvenido al sistema, puede comenzar a escribir. Escriba 'exit' para salir.\n");

	while (enviar) {

		char* entrada;

		entrada = readline("");

		if (strcmp(entrada, "") != 0) {

			char* parametros;
			int header;

			separarEntrada(entrada, &header, &parametros);

			free(parametros);

			if (header == EXIT_CONSOLE) {
				enviar = 0;
			}

			add_history(entrada);

			if (enviar) {

				Script* script_consola = malloc(sizeof(Script));
				script_consola->index = 0;

				script_consola->lineas = string_split(entrada, "\n");
				script_consola->cant_lineas = cant_parametros(
						script_consola->lineas);

				script_a_ready(script_consola);
			}
		}
		free(entrada);

	}

	printf("Desconectado.\n");

	/*	NUNCA nos olvidamos de liberar la memoria que pedimos.
	 *
	 *  Acordate que por cada free() que no hacemos, valgrind mata a un gatito.
	 */
	free(package.message);

	/*
	 *	Listo! Cree un medio de comunicacion con el servidor, me conecte con y le envie cosas...
	 *
	 *	...Pero me aburri. Era de esperarse, No?
	 *
	 *	Asique ahora solo me queda cerrar la conexion con un close();
	 */

	config_destroy(conection_conf);
	close(serverSocket);
	log_destroy(logger_Kernel);
	list_destroy_and_destroy_elements(tablas_actuales, (void*) free);
	list_destroy(memoriasConectadas);
	queue_destroy(colaReady);
	list_destroy_and_destroy_elements(metricas_memorias, (void*) free);

	/* ADIO'! */
	return 0;
}

void abrir_config(t_config** g_config) {

	(*g_config) = config_create(CONFIG_PATH);

}

t_log* iniciar_logger(void) {

	return log_create(LOG_FILE_PATH, "kernel", 1, LOG_LEVEL_DEBUG);

}

int obtenerConsistencia(char* tablaPath) {
	int consistencia = NULL;
	void buscarTabla(Tabla* tabla) {
		if (strcmp(tabla->nombre_tabla, tablaPath) == 0) {
			consistencia = tabla->consistencia;
		}
	}
	pthread_mutex_lock(&tablas_actuales_mutex);
	list_iterate(tablas_actuales, buscarTabla);
	pthread_mutex_unlock(&tablas_actuales_mutex);
	return consistencia;
}

int socketAUtilizar(char* tablaPath, int exec_index, int tipo_consulta) {
	int consistencia = obtenerConsistencia(tablaPath);

	if (consistencia != NULL) {
		return socketFromConsistency(consistencia, exec_index, tipo_consulta);
	}
	return -1;
}

int socketFromConsistency(int consistencia, int exec_index, int tipo_consulta) {
	int num_mem;
	int* temp_mem;
	switch (consistencia) {
	case SC:
		num_mem = strongC;
		break;
	case SHC:
		return -1;
		break;
	case EC:
		pthread_mutex_lock(&eventual_mutex);
		if (queue_size(eventualC) > 0) {
			temp_mem = queue_pop(eventualC);
			num_mem = *temp_mem;
			queue_push(eventualC, temp_mem);
			pthread_mutex_unlock(&eventual_mutex);
		} else {
			pthread_mutex_unlock(&eventual_mutex);
			return -1;
		}
		break;
	}
	int socket = -1;

	void esLaMemoria(Memoria* mem) {
		if (mem->numero == num_mem) {
			socket = mem->socket[exec_index];

		}
	}

	pthread_mutex_lock(&memorias_mutex);
	list_iterate(memoriasConectadas, esLaMemoria);
	pthread_mutex_unlock(&memorias_mutex);

	sumar_metrics_memoria(num_mem,tipo_consulta);

	return socket;
}


void* intentarEstablecerConexion() {
	int serverSocket;
	int num_memoria;
	int conecte;

	while (true) {
		void conectarSiFueraPosible(Seed* seed) {
			conecte = 0;
			if (!seedConectado(seed)) {

				struct addrinfo *serverInfo;
				getaddrinfo(seed->ip, seed->puerto, &hints, &serverInfo);

				Memoria* mem_nueva = malloc(sizeof(Memoria));

				mem_nueva->socket = malloc(sizeof(int) * multiprocesamiento);
				for (int sock = 0; sock < multiprocesamiento; sock++) {
					serverSocket = socket(serverInfo->ai_family,
							serverInfo->ai_socktype, serverInfo->ai_protocol);
					if (connect(serverSocket, serverInfo->ai_addr,
							serverInfo->ai_addrlen) == 0) {

						enviar_handshake(KERNEL, serverSocket);

						num_memoria = recibir_numero_memoria(serverSocket);

						pthread_mutex_lock(&logger_mutex);
						log_debug(logger_Kernel,
								"Conecte a la memoria numero %d", num_memoria);
						pthread_mutex_unlock(&logger_mutex);

						mem_nueva->numero = num_memoria;
						strcpy(mem_nueva->con.puerto, seed->puerto);
						strcpy(mem_nueva->con.ip, seed->ip);
						mem_nueva->socket[sock] = serverSocket;
						conecte = 1;
					}
				}
				freeaddrinfo(serverInfo);

				if (conecte) {
					pthread_mutex_lock(&memorias_mutex);
					list_add(memoriasConectadas, mem_nueva);
					pthread_mutex_unlock(&memorias_mutex);
					crear_metricas(num_memoria);
				} else {
					free(mem_nueva->socket);
					free(mem_nueva);
				}

			}
		}

		pthread_mutex_lock(&gossiping_mutex);
		list_iterate(tablaGossiping, &conectarSiFueraPosible);
		pthread_mutex_unlock(&gossiping_mutex);

		sleep(5);
	}
}

int seedConectado(Seed* seed) {

	int tieneEsePuerto(Memoria* mem) {
		return (strcmp(mem->con.puerto, seed->puerto) == 0
				&& strcmp(mem->con.ip, seed->ip) == 0);
	}

	pthread_mutex_lock(&memorias_mutex);
	int ok = list_any_satisfy(memoriasConectadas, &tieneEsePuerto);
	pthread_mutex_unlock(&memorias_mutex);

	return ok;

}

void desconectar_mem(int socket) {
	close(socket);

	int num;

	int esLaMem(Memoria* mem) {

		for (int sock = 0; sock < multiprocesamiento; sock++) {
			if (mem->socket[sock] == socket) {
				num = mem->numero;
				free(mem->socket);
				return 1;
			}
		}

		return 0;

	}

	pthread_mutex_lock(&memorias_mutex);
	list_remove_by_condition(memoriasConectadas, &esLaMem);
	pthread_mutex_unlock(&memorias_mutex);

	void mostrar(Memoria* mem) {
		pthread_mutex_lock(&logger_mutex);
		log_debug(logger_Kernel, "Mem: %d", mem->numero);
		pthread_mutex_unlock(&logger_mutex);
	}

	pthread_mutex_lock(&memorias_mutex);
	list_iterate(memoriasConectadas, &mostrar);
	pthread_mutex_unlock(&memorias_mutex);

	if (strongC == num) {
		strongC = NULL;
	}

	int esElNum(int* numero) {
		return (*numero) == num;
	}

	pthread_mutex_lock(&eventual_mutex);
	list_remove_by_condition(eventualC->elements, &esElNum);
	pthread_mutex_unlock(&eventual_mutex);

	eliminar_metricas(num);
}

void crear_metricas(int num_mem){

	int esLaMet(MetricaPorMemoria* met) {
			return met->numero_memoria == num_mem;
		}

	if(!list_any_satisfy(metricas_memorias,&esLaMet)){

		MetricaPorMemoria* metrica_nueva = malloc(sizeof(MetricaPorMemoria));

		metrica_nueva->cantidad_insert = 0;
		metrica_nueva->cantidad_select = 0;
		metrica_nueva->numero_memoria = num_mem;
		pthread_mutex_lock(&metricas);
		list_add(metricas_memorias,metrica_nueva);
		pthread_mutex_unlock(&metricas);
	}
}

void eliminar_metricas(int num_mem){

	int esLaMet(MetricaPorMemoria* met) {

		if (met->numero_memoria == num_mem) {
			return 1;
		}
		return 0;
		}

		pthread_mutex_lock(&metricas);
		list_remove_by_condition(metricas_memorias, &esLaMet);
		pthread_mutex_unlock(&metricas);

		void mostrar(MetricaPorMemoria* met) {

		printf( "Metricas Memoria: %d \n", met->numero_memoria);

		}

		pthread_mutex_lock(&metricas);
		list_iterate(metricas_memorias, &mostrar);
		pthread_mutex_unlock(&metricas);
}

int interpretarComando(int header, char* parametros, int exec_index) {
	int result = 1;
	switch (header) {
	case SELECT:
		result = select_kernel(parametros, exec_index);
		break;
	case INSERT:
		result = insert_kernel(parametros, exec_index);
		break;
	case DESCRIBE:
		describe(parametros, exec_index);
		break;
	case DROP:
		drop(parametros, exec_index);
		break;
	case CREATE:
		create(parametros, exec_index);
		break;
	case JOURNAL:
		journal("", exec_index);
		break;
	case RUN:
		return run(parametros, exec_index);
		break;
	case ADD:
		add(parametros, exec_index);
		break;
	case METRICS:
		metrics();
		break;
	case -1:
		break;
	}
	return result;
}

int select_kernel(char* parametros, int exec_index) {

	int ok = 1;
	char *serializedPackage;
	int entradaValida = 1;
	t_PackageSelect package;
	int consistencia;

	if (!fill_package_select(&package, parametros)) {

		printf("Incorrecta cantidad de parametros\n");
		entradaValida = 0;
	}
	if (entradaValida) {
		pthread_mutex_lock(&logger_mutex);
		log_info(logger_Kernel, "SELECT enviado (Tabla: %s, Key: %d)",
				package.tabla, package.key);
		pthread_mutex_unlock(&logger_mutex);

		serializedPackage = serializarSelect(&package);

		int socketAEnviar = socketAUtilizar(package.tabla, exec_index, SELECT);

		if (socketAEnviar != -1) {

			send(socketAEnviar, serializedPackage, package.total_size, 0);

			long timestampInical = (long) time(NULL);

			char* respuesta = recieve_and_deserialize_mensaje(socketAEnviar);

			long timestampDiferencia = (long) time(NULL) - timestampInical;

			if (!(int) respuesta) {
				desconectar_mem(socketAEnviar);
				timestampDiferencia = 0;
				log_error_s(logger_Kernel, "Memoria desconectada");
			} else {
				log_debug_s(logger_Kernel, respuesta);
			}
			//printf("%s\n", respuesta);
			free(respuesta);
			consistencia = obtenerConsistencia(package.tabla);

			sumar_metricas(SELECT, consistencia, timestampDiferencia);

		} else {
			ok = 0;
			printf("Ninguna memoria asignada para este criterio\n");
		}

		free(package.tabla);
		dispose_package(&serializedPackage);
	}
	return ok;
}

int insert_kernel(char* parametros, int exec_index) {

	int ok = 1;
	char* serializedPackage;
	int entradaValida = 1;
	t_PackageInsert package;
	int consistencia;

	if (!fill_package_insert(&package, parametros, 0)) {
		printf("Incorrecta cantidad de parametros\n");
		entradaValida = 0;
	}

	if (entradaValida) {
		pthread_mutex_lock(&logger_mutex);
		log_info(logger_Kernel,
				"INSERT enviado (Tabla: %s, Key: %d, Value: %s, Timestamp: %d)",
				package.tabla, package.key, package.value, package.timestamp);
		pthread_mutex_unlock(&logger_mutex);

		serializedPackage = serializarInsert(&package);

		int socketAEnviar = socketAUtilizar(package.tabla, exec_index, INSERT);
		if (socketAEnviar != -1) {
			send(socketAEnviar, serializedPackage, package.total_size, 0);

			long timestampInical = (long) time(NULL);

			char* respuesta = recieve_and_deserialize_mensaje(socketAEnviar);

			long timestampDiferencia = (long) time(NULL) - timestampInical;

			if (!(int) respuesta) {
				desconectar_mem(socketAEnviar);
				log_error_s(logger_Kernel, "Memoria desconectada");
			} else {
				log_debug_s(logger_Kernel, respuesta);

				pthread_mutex_lock(&logger_mutex);
				log_info(logger_Kernel, "Insert: %s", respuesta);
				pthread_mutex_unlock(&logger_mutex);

				if (strcmp(respuesta, "FULL") == 0) {
					journal("", exec_index);
					send(socketAEnviar, serializedPackage, package.total_size,
							0);
					long timestampInical = (long) time(NULL);

					free(respuesta);
					respuesta = recieve_and_deserialize_mensaje(socketAEnviar);
					long timestampDiferencia = (long) time(NULL)
							- timestampInical;

					if (!(int) respuesta) {
						desconectar_mem(socketAEnviar);
						timestampDiferencia = 0;

						log_error_s(logger_Kernel, "Memoria desconectada");
					} else {
						pthread_mutex_lock(&logger_mutex);
						log_info(logger_Kernel, "Insert: %s", respuesta);
						pthread_mutex_unlock(&logger_mutex);
					}
				}

			}

			free(respuesta);
			consistencia = obtenerConsistencia(package.tabla);
			sumar_metricas(INSERT, consistencia, timestampDiferencia);

			//printf("%s\n", respuesta);

		} else {
			ok = 0;
			printf("Ninguna memoria asignada para este criterio\n");
		}
		free(package.tabla);
		free(package.value);
		dispose_package(&serializedPackage);
	}
	return ok;
}

void describe(char* parametros, int exec_index) {
	char *serializedPackage;
	int entradaValida = 1;
	t_PackageDescribe package;

	if (!fill_package_describe(&package, parametros)) {
		printf("Incorrecta cantidad de parametros\n");
		entradaValida = 0;
	}
	if (entradaValida) {
		log_info_s(logger_Kernel, "DESCRIBE enviado");

		serializedPackage = serializarRequestDescribe(&package);

		pthread_mutex_lock(&memorias_mutex);
		if (!memoriasConectadas->elements_count) {
			pthread_mutex_unlock(&memorias_mutex);
			log_error_s(logger_Kernel, "No hay memorias conectadas");
			return;
		}
		pthread_mutex_unlock(&memorias_mutex);
		int socketAEnviar =
				((Memoria*) list_get(memoriasConectadas, 0))->socket[exec_index];

		if (socketAEnviar != -1) {
			//printf("Lo mande\n");
			send(socketAEnviar, serializedPackage, package.total_size, 0);

			recibirDescribe(socketAEnviar);

		} else {
			printf("Ninguna memoria asignada para este criterio\n");
		}

		dispose_package(&serializedPackage);
	}
}

void sumar_metrics_memoria(int num_mem, int tipo_consulta){


	int es_la_memoria(MetricaPorMemoria* met){
		if (met->numero_memoria == num_mem) {

			return 1;
		}
		return 0;
	}

	void sumarMemoria(Memoria* mem) {
		if (mem->numero == num_mem) {
			MetricaPorMemoria* met_encontrada;
			pthread_mutex_lock(&metricas);
			met_encontrada = (MetricaPorMemoria*)list_find(metricas_memorias, &es_la_memoria);
			pthread_mutex_unlock(&metricas);

			if(tipo_consulta == SELECT){
				met_encontrada->cantidad_select++;
			}else if(tipo_consulta==INSERT){
				met_encontrada->cantidad_insert++;
			}
		}

	}

	pthread_mutex_lock(&memorias_mutex);
	list_iterate(memoriasConectadas, sumarMemoria);
	pthread_mutex_unlock(&memorias_mutex);

}

void sumar_metricas(int tipo_consulta, int consistencia, long tiempo) {


	switch (tipo_consulta) {
	case SELECT:
		select_totales++;
		switch (consistencia) {
		case SC:
			select_sc.cantidad++;
			select_sc.tiempoTotal += tiempo;
			break;

		case EC:
			select_ec.cantidad++;
			select_ec.tiempoTotal += tiempo;
			break;

		case SHC:
			select_shc.cantidad++;
			select_shc.tiempoTotal += tiempo;
			break;

		}
		break;

	case INSERT:
		insert_totales++;
		switch (consistencia) {
		case SC:
			insert_sc.cantidad++;
			insert_sc.tiempoTotal += tiempo;
			break;

		case EC:
			insert_ec.cantidad++;
			insert_ec.tiempoTotal += tiempo;
			break;

		case SHC:
			insert_shc.cantidad++;
			insert_shc.tiempoTotal += tiempo;
			break;

		}
		break;

	}

}

void* metricsCada30() {

	void lockMutexes(pthread_mutex_t* mutex) {
		pthread_mutex_lock(mutex);
	}

	void unLockMutexes(pthread_mutex_t* mutex) {
		pthread_mutex_unlock(mutex);
	}

	while (true) {

		list_iterate(exec_mutexes, &lockMutexes);
    
		metrics(1);

		select_sc.tiempoTotal = 0;
		select_sc.cantidad = 0;

		select_ec.tiempoTotal = 0;
		select_ec.cantidad = 0;

		select_shc.tiempoTotal = 0;
		select_shc.cantidad = 0;

		insert_sc.tiempoTotal = 0;
		insert_sc.cantidad = 0;

		insert_ec.tiempoTotal = 0;
		insert_ec.cantidad = 0;

		insert_shc.tiempoTotal = 0;
		insert_shc.cantidad = 0;

		list_iterate(exec_mutexes, &unLockMutexes);

		sleep(30);
	}
}

void drop(char* parametros, int exec_index) {
	char *serializedPackage;
	int entradaValida = 1;
	t_PackageDrop package;

	if (!fill_package_drop(&package, parametros)) {
		printf("Incorrecta cantidad de parametros\n");
		entradaValida = 0;
	}
	if (entradaValida) {
		log_info_s(logger_Kernel, "DROP enviado");

		serializedPackage = serializarDrop(&package);

		pthread_mutex_lock(&memorias_mutex);
		if (!memoriasConectadas->elements_count) {
			pthread_mutex_unlock(&memorias_mutex);
			log_error_s(logger_Kernel, "No hay memorias conectadas");
			return;
		}
		pthread_mutex_unlock(&memorias_mutex);
		int socketAEnviar = socketAUtilizar(package.nombre_tabla, exec_index,
		NULL);

		if (socketAEnviar != -1) {
			//printf("Lo mande\n");
			send(socketAEnviar, serializedPackage, package.total_size, 0);
			eliminarTabla(package.nombre_tabla);

		} else {
			printf("Ninguna memoria asignada para este criterio\n");
		}

		dispose_package(&serializedPackage);

	}
}

void eliminarTabla(char* nombre) {

	int esDeLaTabla(Tabla *tabla) {
		if (strcmp(tabla->nombre_tabla, nombre) == 0) {
			return 1;
		}
		return 0;
	}
	;

	pthread_mutex_lock(&tablas_actuales_mutex);
	list_remove_and_destroy_by_condition(tablas_actuales, &esDeLaTabla,
			(void*) free);
	pthread_mutex_unlock(&tablas_actuales_mutex);

}

void recibirDescribe(int serverSocket) {

	t_describe describe;

	if (!(int) recieve_and_deserialize_describe(&describe, serverSocket)) {
		desconectar_mem(serverSocket);
		log_error_s(logger_Kernel, "Memoria desconectada");
	} else {
		if (strcmp(describe.tablas[0].nombre_tabla, "NO_TABLE") == 0) {
			log_warning_s(logger_Kernel, "La tabla no existe");
		} else {
			for (int i = 0; i < describe.cant_tablas; i++) {
				//printf("%s\n", describe.tablas[i].nombre_tabla);
				if (obtenerConsistencia(describe.tablas[i].nombre_tabla) == NULL) {
					Tabla* tabla_nueva = malloc(sizeof(Tabla));
					strcpy(tabla_nueva->nombre_tabla,
							describe.tablas[i].nombre_tabla);
					tabla_nueva->consistencia = describe.tablas[i].consistencia;
					pthread_mutex_lock(&tablas_actuales_mutex);
					list_add(tablas_actuales, tabla_nueva);
					pthread_mutex_unlock(&tablas_actuales_mutex);
				}
			}

			/*
			 pthread_mutex_lock(&tablas_actuales_mutex);
			 for (int tabla2 = 0; tabla2 < tablas_actuales->elements_count;
			 tabla2++) {
			 Tabla* tabla = list_get(tablas_actuales, tabla2);
			 printf("Tabla %s \n", tabla->nombre_tabla);
			 printf("Consistencia %s \n",
			 consistency_to_str(tabla->consistencia));
			 }
			 pthread_mutex_unlock(&tablas_actuales_mutex);
			 */
		}
		free(describe.tablas);
	}

}

void create(char* parametros, int exec_index) {
	char* serializedPackage;
	int entradaValida = 1;
	t_PackageCreate package;

	if (!fill_package_create(&package, parametros)) {
		printf("Incorrecta cantidad de parametros\n");
		entradaValida = 0;
	}

	if (entradaValida) {
		pthread_mutex_lock(&logger_mutex);
		log_info(logger_Kernel, "CREATE enviado (Tabla: %s, CONSISTENCY: %s)",
				package.tabla, consistency_to_str(package.consistency));
		pthread_mutex_unlock(&logger_mutex);

		serializedPackage = serializarCreate(&package);

		pthread_mutex_lock(&memorias_mutex);
		if (!memoriasConectadas->elements_count) {
			pthread_mutex_unlock(&memorias_mutex);
			log_error_s(logger_Kernel, "No hay memorias conectadas");
			return;
		}
		pthread_mutex_unlock(&memorias_mutex);

		pthread_mutex_lock(&memorias_mutex);
		int socketAEnviar =
				((Memoria*) list_get(memoriasConectadas, 0))->socket[exec_index];
		pthread_mutex_unlock(&memorias_mutex);

		send(socketAEnviar, serializedPackage, package.total_size, 0);

		free(package.tabla);
		dispose_package(&serializedPackage);
	}

}

void journal(char* parametros, int exec_index) {
	char* serializedPackage;
	int header = JOURNAL;
	serializedPackage = malloc(sizeof(int));

	memcpy(serializedPackage, &header, sizeof(int));

	void enviarJournal(Memoria* mem) {
		send(mem->socket[exec_index], serializedPackage, sizeof(int), 0);
	}

	pthread_mutex_lock(&memorias_mutex);
	list_iterate(memoriasConectadas, &enviarJournal);
	pthread_mutex_unlock(&memorias_mutex);

	dispose_package(&serializedPackage);
}

void seedPackageToTable(t_PackageSeeds* seeds) {
	pthread_mutex_lock(&gossiping_mutex);
	for (int i = 0; i < seeds->cant_seeds; i++) {

		int estaLaSeed(Seed* seed) {
			return (strcmp(seed->puerto, seeds->seeds[i].puerto) == 0
					&& strcmp(seed->ip, seeds->seeds[i].ip) == 0);
		}

		if (!list_any_satisfy(tablaGossiping, &estaLaSeed)) {
			Seed* seed = malloc(sizeof(Seed));
			strcpy(seed->puerto, seeds->seeds[i].puerto);
			strcpy(seed->ip, seeds->seeds[i].ip);
			list_add(tablaGossiping, seed);
		};
	}
	pthread_mutex_unlock(&gossiping_mutex);
}

void *intercambiarTabla() {
	int serverSocket;

	while (true) {

		struct addrinfo *serverInfo;

		pthread_mutex_lock(&memorias_mutex);
		Memoria* mem = (Memoria*) list_get(memoriasConectadas, 0);
		getaddrinfo(mem->con.ip, mem->con.puerto, &hints, &serverInfo);
		pthread_mutex_unlock(&memorias_mutex);

		serverSocket = socket(serverInfo->ai_family, serverInfo->ai_socktype,
				serverInfo->ai_protocol);
		if (connect(serverSocket, serverInfo->ai_addr, serverInfo->ai_addrlen)
				== 0) {

			enviar_handshake(KERNEL, serverSocket);
			int num_memoria = recibir_numero_memoria(serverSocket);

			char* serializedPackage;
			int header = GOSSIPING;
			serializedPackage = malloc(sizeof(int));

			memcpy(serializedPackage, &header, sizeof(int));

			send(serverSocket, serializedPackage, sizeof(int), 0);

			t_PackageSeeds* seeds = malloc(sizeof(t_PackageSeeds));
			recieve_and_deserialize_gossipingTable(seeds, serverSocket);

			seedPackageToTable(seeds);

			free(serializedPackage);
			free(seeds->seeds);
			free(seeds);
			close(serverSocket);
		}
		freeaddrinfo(serverInfo);

		sleep(5);
	}
}

void *describeNuevo() {
	int serverSocket;

	while (true) {

		struct addrinfo *serverInfo;

		pthread_mutex_lock(&memorias_mutex);
		Memoria* mem = (Memoria*) list_get(memoriasConectadas, 0);
		getaddrinfo(mem->con.ip, mem->con.puerto, &hints, &serverInfo);
		pthread_mutex_unlock(&memorias_mutex);

		serverSocket = socket(serverInfo->ai_family, serverInfo->ai_socktype,
				serverInfo->ai_protocol);
		if (connect(serverSocket, serverInfo->ai_addr, serverInfo->ai_addrlen)
				== 0) {

			enviar_handshake(KERNEL, serverSocket);
			int num_memoria = recibir_numero_memoria(serverSocket);

			char *serializedPackage;
			int entradaValida = 1;
			t_PackageDescribe package;

			serializedPackage = serializarRequestDescribe(&package);

			send(serverSocket, serializedPackage, package.total_size, 0);

			recibirDescribe(serverSocket);

			dispose_package(&serializedPackage);
		}

		close(serverSocket);

		freeaddrinfo(serverInfo);

		pthread_mutex_lock(&config_mutex);
		usleep(tiempoDescribe);
		pthread_mutex_unlock(&config_mutex);
	}
}

void* describeCadaX(int serverSocket) {
// tiempoDescribe = config_get_int_value(conection_conf,"METADATA_REFRESH")*1000;

	void lockMutexes(pthread_mutex_t* mutex) {
		pthread_mutex_lock(mutex);
	}

	void unLockMutexes(pthread_mutex_t* mutex) {
		pthread_mutex_unlock(mutex);
	}

	while (true) {
		pthread_mutex_lock(&config_mutex);
		usleep(tiempoDescribe);
		pthread_mutex_unlock(&config_mutex);

		list_iterate(exec_mutexes, &lockMutexes);

		interpretarComando(DESCRIBE, NULL, 0);

		list_iterate(exec_mutexes, &unLockMutexes);
	}

}

void add(char* parametros, int serverSocket) {
	char** parametrosSeparados = string_split(parametros, " ");
	if (cant_parametros(parametrosSeparados) != 4) {
		printf("Cantidad de parametros invalidos\n");
	} else {
		int num_mem = atoi(parametrosSeparados[1]);
		int consistency = consistency_to_int(parametrosSeparados[3]);
		int esta = 0;
		int estaLaMem(Memoria* mem) {
			if (mem->numero == num_mem) {
				esta = 1;
			}
		}

		pthread_mutex_lock(&memorias_mutex);
		list_iterate(memoriasConectadas, estaLaMem);
		pthread_mutex_unlock(&memorias_mutex);

		int* num_mem_puntero;

		if (esta) {
			switch (consistency) {
			case SC:
				strongC = num_mem;
				break;
			case SHC:
				break;
			case EC:
				num_mem_puntero = malloc(sizeof(int));
				memcpy(num_mem_puntero, &num_mem, sizeof(int));
				pthread_mutex_lock(&eventual_mutex);
				queue_push(eventualC, num_mem_puntero);
				pthread_mutex_unlock(&eventual_mutex);
				break;
			}
		} else {
			log_error_s(logger_Kernel, "Esa memoria no esta conectada");
		}
	}

	string_iterate_lines(parametrosSeparados, (void*) free);
	free(parametrosSeparados);
}

void metrics() {

	void mostrarMemoria(MetricaPorMemoria* met) {
		log_debug(logger_Kernel, "Memoria: %d", met->numero_memoria);
		log_debug(logger_Kernel, "Insert: %d/%d", met->cantidad_insert, insert_totales);
		//printf("Insert: %d/%d \n", met->cantidad_insert, insert_totales);
		log_debug(logger_Kernel, "Select: %d/%d \n", met->cantidad_select ,select_totales);
		//printf("Select: %d/%d \n", met->cantidad_select ,select_totales);
	}

	float tiempoPromedio;

//Read Latency
	if (select_sc.cantidad == 0) {
		tiempoPromedio = 0;
	} else {
		tiempoPromedio = (double) select_sc.tiempoTotal
				/ (double) select_sc.cantidad;
	}
	log_debug(logger_Kernel, "Latencies:");
	log_debug(logger_Kernel, "Read Latency SC %f", tiempoPromedio);
	//printf("Read Latency SC %f \n", tiempoPromedio);

	if (select_ec.cantidad == 0) {
		tiempoPromedio = 0;
	} else {
		tiempoPromedio = (double) select_ec.tiempoTotal
				/ (double) select_ec.cantidad;
	}
	log_debug(logger_Kernel, "Read Latency EC %f", tiempoPromedio);
	//printf("Read Latency EC %f \n", tiempoPromedio);

	if (select_shc.cantidad == 0) {
		tiempoPromedio = 0;
	} else {
		tiempoPromedio = (double) select_shc.tiempoTotal
				/ (double) select_shc.cantidad;
	}
	log_debug(logger_Kernel, "Read Latency SHC %f", tiempoPromedio);
	//printf("Read Latency SHC %f \n", tiempoPromedio);

//Write Latency
	if (insert_sc.cantidad == 0) {
		tiempoPromedio = 0;
	} else {
		tiempoPromedio = (double) insert_sc.tiempoTotal
				/ (double) insert_sc.cantidad;
	}
	log_debug(logger_Kernel, "Write Latency SC %f", tiempoPromedio);
	//printf("Write Latency SC %f \n", tiempoPromedio);

	if (insert_ec.cantidad == 0) {
		tiempoPromedio = 0;
	} else {
		tiempoPromedio = (double) insert_ec.tiempoTotal
				/ (double) insert_ec.cantidad;

	}

	log_debug(logger_Kernel, "Write Latency EC %f", tiempoPromedio);
	//printf("Write Latency EC %f \n", tiempoPromedio);

	if (insert_shc.cantidad == 0) {
		tiempoPromedio = 0;
	} else {
		tiempoPromedio = (double) insert_shc.tiempoTotal
				/ (double) insert_shc.cantidad;

	}
	log_debug(logger_Kernel, "Write Latency SHC %f \n", tiempoPromedio);
	//printf("Write Latency SHC %f \n", tiempoPromedio);

	//Reads
	//printf("Reads SC: %d \n", select_sc.cantidad);
	log_debug(logger_Kernel, "Reads:");
	log_debug(logger_Kernel, "Reads SC: %d", select_sc.cantidad);
	//printf("Reads EC: %d \n", select_ec.cantidad);
	log_debug(logger_Kernel, "Reads EC: %d", select_ec.cantidad);
	//printf("Reads SHC: %d \n", select_shc.cantidad);
	log_debug(logger_Kernel, "Reads SHC: %d \n", select_shc.cantidad);

	//Writes
	//printf("Writes SC: %d \n", insert_sc.cantidad);
	log_debug(logger_Kernel, "Writes:");
	log_debug(logger_Kernel, "Writes SC: %d", insert_sc.cantidad);
	//printf("Writes EC: %d \n", insert_ec.cantidad);
	log_debug(logger_Kernel, "Writes EC: %d", insert_ec.cantidad);
	//printf("Writes SHC: %d \n", insert_shc.cantidad);
	log_debug(logger_Kernel, "Writes SHC: %d \n", insert_shc.cantidad);

	//Memory Loads
	pthread_mutex_lock(&metricas);
	list_iterate(metricas_memorias, &mostrarMemoria);
	pthread_mutex_unlock(&metricas);

}

int run(char* rutaRecibida, int serverSocket) {

//char* rutaArchivo = malloc(strlen(rutaRecibida));
//strcpy(rutaArchivo, rutaRecibida);

	printf("%s viajando a new!\n", rutaRecibida);

	Script* test = levantar_script(rutaRecibida);

	if (test->lineas != NULL) {
		script_a_ready(test);
	} else {
		free(test);
		return 0;
	}
//free(rutaArchivo);
	return 1;
}

Script* levantar_script(char* ruta) {

	Script* nuevoScript = malloc(sizeof(Script));
	nuevoScript->index = 0;
	char* f;
	char* lineasnuevas;

	int fd = open(ruta, O_RDONLY, S_IRUSR | S_IWUSR);

	if (fd == -1) {
		nuevoScript->lineas = NULL;
		return nuevoScript;
	}

	struct stat s;
	int status = fstat(fd, &s);
	int size = s.st_size;

	f = (char *) mmap(NULL, size, PROT_READ, MAP_PRIVATE, fd, 0);

	lineasnuevas = malloc(size + 1);

	memcpy(lineasnuevas, f, size);

	lineasnuevas[size] = '\0';
	munmap(f, size);
	close(fd);

	nuevoScript->lineas = string_split(lineasnuevas, "\n");

	free(lineasnuevas);

	nuevoScript->cant_lineas = cant_parametros(nuevoScript->lineas);

//log_info_s(logger_Kernel, string_itoa(nuevoScript->cant_lineas));

	return nuevoScript;
}

int is_regular_file(const char *path) {

	struct stat path_stat;
	stat(path, &path_stat);
	return S_ISREG(path_stat.st_mode); //Devuelve verdadero si la ruta es un archivo
}

void script_a_ready(Script* script) {

	pthread_mutex_lock(&cola_ready_mutex);
	queue_push(colaReady, script);
	pthread_mutex_unlock(&cola_ready_mutex);

	sem_post(&ejecutar_sem);

}

void* exec(int index) {
	int serverSocket = 1;
	int resultado_exec;
	while (true) {

		sem_wait(&ejecutar_sem);

		pthread_mutex_lock(list_get(exec_mutexes, index));

		pthread_mutex_lock(&cola_ready_mutex);
		Script* script_en_ejecucion = (Script*) queue_pop(colaReady);
		pthread_mutex_unlock(&cola_ready_mutex);

		resultado_exec = ejecutar_quantum(&script_en_ejecucion, index);

		switch (resultado_exec) {
		case CORTE_SCRIPT_POR_FINALIZACION:
			log_info_s(logger_Kernel, "Finalizó");
			string_iterate_lines(script_en_ejecucion->lineas, (void*) free);
			free(script_en_ejecucion->lineas);
			free(script_en_ejecucion);
			break;
		case CORTE_SCRIPT_POR_FIN_QUANTUM:
			log_info_s(logger_Kernel, "Finalizó quantum");
			script_a_ready(script_en_ejecucion);
			break;
		case CORTE_SCRIPT_POR_LINEA_ERRONEA:
			log_error_s(logger_Kernel, "Script terminado por linea erronea");
			string_iterate_lines(script_en_ejecucion->lineas, (void*) free);
			free(script_en_ejecucion->lineas);
			free(script_en_ejecucion);
			break;
		}
		pthread_mutex_unlock(list_get(exec_mutexes, index));
	}
}

void log_debug_s(t_log* log, char* mensaje) {
	pthread_mutex_lock(&logger_mutex);
	log_debug(log, mensaje);
	pthread_mutex_unlock(&logger_mutex);
}
void log_warning_s(t_log* log, char* mensaje) {
	pthread_mutex_lock(&logger_mutex);
	log_warning(log, mensaje);
	pthread_mutex_unlock(&logger_mutex);
}
void log_error_s(t_log* log, char* mensaje) {
	pthread_mutex_lock(&logger_mutex);
	log_error(log, mensaje);
	pthread_mutex_unlock(&logger_mutex);
}
void log_info_s(t_log* log, char* mensaje) {
	pthread_mutex_lock(&logger_mutex);
	log_info(log, mensaje);
	pthread_mutex_unlock(&logger_mutex);
}

int ejecutar_quantum(Script** script, int index) {
	Script* scriptEnExec = *script;
	int entradaValida;
	int ejecutadas = 1;
	int ejecucionCorrecta = 1;
	int header;
//pthread_mutex_lock(&config_mutex);
	do {
		printf("Ejecutando un quantum \n");
		printf("%s \n", scriptEnExec->lineas[scriptEnExec->index]);

		ejecucionCorrecta = 1;
		entradaValida = 1;
		char* entrada = scriptEnExec->lineas[scriptEnExec->index];

		char* parametros;

		separarEntrada(entrada, &header, &parametros);

		entradaValida = validarParametros(header, parametros);

		if (header == ERROR) {
			log_warning_s(logger_Kernel, "Comando no reconocido");
			entradaValida = 0;
		}

		if (entradaValida) {
			ejecucionCorrecta = interpretarComando(header, parametros, index);
		}
		if (!entradaValida || !ejecucionCorrecta) {
			return CORTE_SCRIPT_POR_LINEA_ERRONEA;
		}
		free(parametros);
		ejecutadas++;

		scriptEnExec->index++;

	} while ((scriptEnExec->index < scriptEnExec->cant_lineas)
			&& (ejecutadas <= quantum));
//pthread_mutex_unlock(&config_mutex);
	if (scriptEnExec->index == scriptEnExec->cant_lineas) {
		return CORTE_SCRIPT_POR_FINALIZACION;
	}
	return CORTE_SCRIPT_POR_FIN_QUANTUM;
}

void* watch_config(char* config) {
	int wd, fd;

	fd = inotify_init();
	if (fd < 0) {
		perror("Couldn't initialize inotify");
	}

	wd = inotify_add_watch(fd, ".", IN_CREATE | IN_MODIFY | IN_DELETE);
	if (wd == -1) {
		printf("Couldn't add watch to %s\n", config);
	} else {
		printf("Watching:: %s\n", config);
	}

	/* do it forever*/
	while (1) {
		get_event(fd);
	}

	/* Clean up*/
	inotify_rm_watch(fd, wd);
	close(fd);

}

void get_event(int fd) {

	char buffer[BUF_LEN];
	int length, i = 0;

	length = read(fd, buffer, BUF_LEN);
	if (length < 0) {
		perror("read");
	}

	while (i < length) {
		struct inotify_event *event = (struct inotify_event *) &buffer[i];
		if (event->len && !strcmp(event->name, CONFIG_PATH)) {
			if (event->mask & IN_MODIFY) {
				log_info(logger_Kernel, "Antes: %i", metadata_refresh);
				log_info(logger_Kernel, "Antes: %i", quantum);
				log_info(logger_Kernel, "Se modifico la config");
				pthread_mutex_lock(&config_mutex);
				config_destroy(conection_conf);
				conection_conf = config_create(CONFIG_PATH);
				log_info(logger_Kernel, "Cree de nuevo la config");
				metadata_refresh = config_get_int_value(conection_conf,
						"METADATA_REFRESH");
				quantum = config_get_int_value(conection_conf, "QUANTUM");
				log_info(logger_Kernel, "Despues: %i", metadata_refresh);
				log_info(logger_Kernel, "Despues: %i", quantum);
				pthread_mutex_unlock(&config_mutex);
			}

		}
		i += EVENT_SIZE + event->len;
	}
}
