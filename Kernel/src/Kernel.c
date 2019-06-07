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
#include <sys/mman.h>
t_log* logger_Kernel;
int quantum;
t_queue* colaReady;
t_queue* colaExec;
sem_t ejecutar_sem;
pthread_mutex_t cola_ready_mutex;
pthread_mutex_t exec_mutex;

char* ip_destino;
char** puertos_posibles;
struct addrinfo hints;

int strongC = NULL;
t_queue* eventualC;

int main() {

	/*
	 *
	 *  Estas y otras preguntas existenciales son resueltas getaddrinfo();
	 *
	 *  Obtiene los datos de la direccion de red y lo guarda en serverInfo.
	 *
	 */
	char* puerto;

	logger_Kernel = iniciar_logger();

	memset(&hints, 0, sizeof(hints));
	hints.ai_family = AF_UNSPEC; // Permite que la maquina se encargue de verificar si usamos IPv4 o IPv6
	hints.ai_socktype = SOCK_STREAM;	// Indica que usaremos el protocolo TCP

	t_config *conection_conf;
	abrir_config(&conection_conf);

	char* ip = config_get_string_value(conection_conf, "IP");
	ip_destino = malloc(strlen(ip));
	strcpy(ip_destino, ip);

	puerto = config_get_string_value(conection_conf, "PUERTO_MEMORIA");
	quantum = config_get_int_value(conection_conf, "QUANTUM");
	printf("LLegue\n");
	puertos_posibles = config_get_array_value(conection_conf, "PUERTOS");
	printf("LLegue\n");
	log_info(logger_Kernel, puerto);

	Memoria* mem_nueva = malloc(sizeof(Memoria));
	mem_nueva->puerto = atoi(puerto);

	struct addrinfo *serverInfo;
	getaddrinfo(ip, puerto, &hints, &serverInfo);// Carga en serverInfo los datos de la conexion

	config_destroy(conection_conf);

	/*
	 * 	Ya se quien y a donde me tengo que conectar... ������Y ahora?
	 *	Tengo que encontrar una forma por la que conectarme al server... Ya se! Un socket!
	 *
	 * 	Obtiene un socket (un file descriptor -todo en linux es un archivo-), utilizando la estructura serverInfo que generamos antes.
	 *
	 */
	int serverSocket;
	serverSocket = socket(serverInfo->ai_family, serverInfo->ai_socktype,
			serverInfo->ai_protocol);

	/*
	 * 	Perfecto, ya tengo el medio para conectarme (el archivo), y ya se lo pedi al sistema.
	 * 	Ahora me conecto!
	 *
	 */

	if (connect(serverSocket, serverInfo->ai_addr, serverInfo->ai_addrlen)
			== 0) {

		enviar_handshake(KERNEL, serverSocket);

		int num_memoria = recibir_numero_memoria(serverSocket);
		printf("Num memoria %d\n", num_memoria);

		printf("Conectado al servidor.\n");
		log_info(logger_Kernel, "Conecte al servidor.");

		mem_nueva->numero = num_memoria;
		mem_nueva->socket = serverSocket;

	} else {
		printf("No se pudo conectar al servidor...");
		log_info(logger_Kernel, "No me pude conectar con el servidor");
	}
	freeaddrinfo(serverInfo);	// No lo necesitamos mas

	printf("Esperando describe.\n");

	tablas_actuales = list_create();

	memoriasConectadas = list_create();

	list_add(memoriasConectadas, mem_nueva);

	//setup planificacion
	colaReady = queue_create();
	colaExec = queue_create();
	sem_init(&ejecutar_sem, 0, 0);
	pthread_mutex_init(&cola_ready_mutex, NULL);
	pthread_mutex_init(&exec_mutex, NULL);
	//setup planificacion

	//setup consistencias
	eventualC = queue_create();
	//setup consistencias

	//thread executer
	pthread_t threadL;
	int iret1;

	iret1 = pthread_create(&threadL, NULL, exec, (void*) serverSocket);
	if (iret1) {
		fprintf(stderr, "Error - pthread_create() return code: %d\n", iret1);
		exit(EXIT_FAILURE);
	}
	//thread executer

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
		fprintf(stderr, "Error - pthread_create() return code: %d\n", iret1);
		exit(EXIT_FAILURE);
	}
	//threadConexiones

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
		char* entrada = leerConsola();

		char* parametros;
		int header;

		separarEntrada(entrada, &header, &parametros);

		if (header == EXIT_CONSOLE) {
			enviar = 0;
		}

		if (enviar) {

			Script* script_consola = malloc(sizeof(Script));
			script_consola->index = 0;

			script_consola->lineas = string_split(entrada, "\n");
			script_consola->cant_lineas = cant_parametros(
					script_consola->lineas);

			script_a_ready(script_consola);
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

	close(serverSocket);
	log_destroy(logger_Kernel);
	list_destroy(tablas_actuales);
	list_destroy(memoriasConectadas);

	queue_destroy(colaReady);
	queue_destroy(colaExec);

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

	list_iterate(tablas_actuales, buscarTabla);
	return consistencia;
}

int socketAUtilizar(char* tablaPath) {
	int consistencia = obtenerConsistencia(tablaPath);

	if (consistencia != NULL) {
		return socketFromConsistency(consistencia);
	}
	return -1;
}

int socketFromConsistency(int consistencia) {
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
		if (queue_size(eventualC) > 0) {
			temp_mem = queue_pop(eventualC);
			num_mem = *temp_mem;
			queue_push(eventualC, temp_mem);
		} else {
			return -1;
		}
		break;
	}
	int socket = -1;

	void esLaMemoria(Memoria* mem) {
		if (mem->numero == num_mem) {
			socket = mem->socket;
		}
	}

	list_iterate(memoriasConectadas, esLaMemoria);
	return socket;
}

void* intentarEstablecerConexion() {
	int i;
	int serverSocket;
	int num_memoria;

	while (true) {

		i = 0;

		while (puertos_posibles[i] != NULL) {

			if (!puertoConectado(puertos_posibles[i])) {

				struct addrinfo *serverInfo;
				getaddrinfo(ip_destino, puertos_posibles[i], &hints,
						&serverInfo);
				serverSocket = socket(serverInfo->ai_family,
						serverInfo->ai_socktype, serverInfo->ai_protocol);

				if (connect(serverSocket, serverInfo->ai_addr,
						serverInfo->ai_addrlen) == 0) {

					enviar_handshake(KERNEL, serverSocket);

					num_memoria = recibir_numero_memoria(serverSocket);

					log_debug(logger_Kernel, "Conecte a la memoria numero %d",
							num_memoria);

					Memoria* mem_nueva = malloc(sizeof(Memoria));
					mem_nueva->numero = num_memoria;
					mem_nueva->puerto = atoi(puertos_posibles[i]);
					mem_nueva->socket = serverSocket;

					list_add(memoriasConectadas, mem_nueva);

				}
				freeaddrinfo(serverInfo);
			}

			i++;
		}
		sleep(5);
	}
}

int puertoConectado(char* puertoChar) {
	int puerto = atoi(puertoChar);

	int tieneEsePuerto(Memoria* mem) {
		return mem->puerto == puerto;
	}

	return list_any_satisfy(memoriasConectadas, tieneEsePuerto);

}

void interpretarComando(int header, char* parametros, int serverSocket) {

	switch (header) {
	case SELECT:
		select_kernel(parametros, serverSocket);
		break;
	case INSERT:
		insert_kernel(parametros, serverSocket);
		break;
	case DESCRIBE:
		describe(parametros, serverSocket);
		break;
	case DROP:
		drop(parametros, serverSocket);
		break;
	case CREATE:
		create(parametros, serverSocket);
		break;
	case JOURNAL:
		journal(parametros, serverSocket);
		break;
	case RUN:

		if (cant_parametros(string_split(parametros, " ")) != 1) {
			printf("Cantidad de parametros invalidos\n");
		} else {
			run(parametros, serverSocket);
		}
		break;
	case ADD:
		add(parametros, serverSocket);
		break;
	case 9:
		metrics(parametros, serverSocket);
		break;
	case -1:
		break;
	}

}

void select_kernel(char* parametros, int serverSocket) {

	char *serializedPackage;
	int entradaValida = 1;
	t_PackageSelect package;

	if (!fill_package_select(&package, parametros)) {

		printf("Incorrecta cantidad de parametros\n");
		entradaValida = 0;
	}
	if (entradaValida) {
		log_info(logger_Kernel,"SELECT enviado (Tabla: %s, Key: %d)", package.tabla,
				package.key);

		serializedPackage = serializarSelect(&package);

		int socketAEnviar = socketAUtilizar(package.tabla);

		if (socketAEnviar != -1) {
			send(socketAEnviar, serializedPackage, package.total_size, 0);

			char* respuesta = recieve_and_deserialize_mensaje(socketAEnviar);
			log_debug(logger_Kernel, respuesta);
			//printf("%s\n", respuesta);
			free(respuesta);

		} else {
			printf("Ninguna memoria asignada para este criterio\n");
		}

		free(package.tabla);
		dispose_package(&serializedPackage);
	}
}

void insert_kernel(char* parametros, int serverSocket) {

	char* serializedPackage;
	int entradaValida = 1;
	t_PackageInsert package;

	if (!fill_package_insert(&package, parametros, 0)) {
		printf("Incorrecta cantidad de parametros\n");
		entradaValida = 0;
	}

	if (entradaValida) {
		log_info(logger_Kernel,
				"INSERT enviado (Tabla: %s, Key: %d, Value: %s, Timestamp: %d)",
				package.tabla, package.key, package.value, package.timestamp);

		serializedPackage = serializarInsert(&package);

		int socketAEnviar = socketAUtilizar(package.tabla);
		if (socketAEnviar != -1) {
			send(socketAEnviar, serializedPackage, package.total_size, 0);
		} else {
			printf("Ninguna memoria asignada para este criterio\n");
		}
		free(package.tabla);
		dispose_package(&serializedPackage);
	}
}

void describe(char* parametros, int serverSocket) {
	char *serializedPackage;
	int entradaValida = 1;
	t_PackageDescribe package;

	if (!fill_package_describe(&package, parametros)) {
		printf("Incorrecta cantidad de parametros\n");
		entradaValida = 0;
	}
	if (entradaValida) {
		log_info(logger_Kernel,"DESCRIBE enviado");

		serializedPackage = serializarRequestDescribe(&package);

		int socketAEnviar = ((Memoria*) list_get(memoriasConectadas, 0))->socket;

		if (socketAEnviar != -1) {
			//printf("Lo mande\n");
			send(socketAEnviar, serializedPackage, package.total_size, 0);

			recibirDescribe(serverSocket);

		} else {
			printf("Ninguna memoria asignada para este criterio\n");
		}

		free(package.nombre_tabla);
		dispose_package(&serializedPackage);
	}
}

void drop(char* parametros, int serverSocket) {
	printf("Recibi un drop.\n");
}

void recibirDescribe(int serverSocket) {

	t_describe describe;
	recieve_and_deserialize_describe(&describe, serverSocket);

	if (strcmp(describe.tablas[0].nombre_tabla, "NO_TABLE") == 0) {
		log_warning(logger_Kernel,"La tabla no existe");
	} else {
		for (int i = 0; i < describe.cant_tablas; i++) {
			//printf("%s\n", describe.tablas[i].nombre_tabla);
			if (obtenerConsistencia(describe.tablas[i].nombre_tabla) == NULL) {
				Tabla* tabla_nueva = malloc(sizeof(Tabla));
				strcpy(tabla_nueva->nombre_tabla,
						describe.tablas[i].nombre_tabla);
				tabla_nueva->consistencia = describe.tablas[i].consistencia;
				list_add(tablas_actuales, tabla_nueva);
			}
		}

		for (int tabla2 = 0; tabla2 < tablas_actuales->elements_count;
				tabla2++) {
			Tabla* tabla = list_get(tablas_actuales, tabla2);
			printf("Tabla %s \n", tabla->nombre_tabla);
			printf("Consistencia %s \n",
					consistency_to_str(tabla->consistencia));
		}
	}
	free(describe.tablas);
}

void create(char* parametros, int serverSocket) {
	char* serializedPackage;
	int entradaValida = 1;
	t_PackageCreate package;

	if (!fill_package_create(&package, parametros)) {
		printf("Incorrecta cantidad de parametros\n");
		entradaValida = 0;
	}

	if (entradaValida) {
		log_info(logger_Kernel,"CREATE enviado (Tabla: %s, CONSISTENCY: %s)", package.tabla,
				consistency_to_str(package.consistency));

		serializedPackage = serializarCreate(&package);

		int socketAEnviar = ((Memoria*) list_get(memoriasConectadas, 0))->socket;

		send(socketAEnviar, serializedPackage, package.total_size, 0);

		free(package.tabla);
		dispose_package(&serializedPackage);
	}

}

void journal(char* parametros, int serverSocket) {
	printf("Recibi un journal.\n");
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

		list_iterate(memoriasConectadas, estaLaMem);

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
				queue_push(eventualC, num_mem_puntero);
				break;
			}
		} else {
			log_error(logger_Kernel,"Esa memoria no esta conectada");
		}
	}

}

void metrics(char* parametros, int serverSocket) {
	printf("Recibi un metrics.\n");
}

void run(char* rutaRecibida, int serverSocket) {

	char* rutaArchivo = malloc(strlen(rutaRecibida));
	strcpy(rutaArchivo, rutaRecibida);

	printf("%s viajando a new!\n", rutaArchivo);

	Script* test = levantar_script(rutaArchivo);

	script_a_ready(test);

	free(rutaArchivo);

}

Script* levantar_script(char* ruta) {

	Script* nuevoScript = malloc(sizeof(Script));
	nuevoScript->index = 0;
	char* f;
	char* lineasnuevas;

	int fd = open(ruta, O_RDONLY, S_IRUSR | S_IWUSR);

	struct stat s;
	int status = fstat(fd, &s);
	int size = s.st_size;


	printf("1\n");
	f = mmap(NULL, size, PROT_READ, MAP_PRIVATE, fd, 0);

	printf("2\n");
	lineasnuevas = malloc(strlen(f));
	strcpy(lineasnuevas,f);
	printf("3\n");
	munmap(f,size);
	close(fd);
	printf("4\n");
	nuevoScript->lineas = string_split(lineasnuevas, "\n");

	nuevoScript->cant_lineas = cant_parametros(nuevoScript->lineas);

	//log_info(logger_Kernel, string_itoa(nuevoScript->cant_lineas));

	printf("5\n");
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

void* exec(int serverSocket) {
	int resultado_exec;
	while (true) {

		sem_wait(&ejecutar_sem);

		pthread_mutex_lock(&exec_mutex);

		pthread_mutex_lock(&cola_ready_mutex);
		Script* script_en_ejecucion = (Script*) queue_pop(colaReady);
		pthread_mutex_unlock(&cola_ready_mutex);

		resultado_exec = ejecutar_quantum(&script_en_ejecucion, serverSocket);

		switch (resultado_exec) {
		case CORTE_SCRIPT_POR_FINALIZACION:
			//log_info(logger_Kernel, script_en_ejecucion->lineas[script_en_ejecucion->index-1]);
			log_info(logger_Kernel, "Finalizó");
			free(script_en_ejecucion->lineas);
			free(script_en_ejecucion);
			break;
		case CORTE_SCRIPT_POR_FIN_QUANTUM:
			log_info(logger_Kernel, "Finalizó quantum");
			script_a_ready(script_en_ejecucion);
			break;
		case CORTE_SCRIPT_POR_LINEA_ERRONEA:
			log_error(logger_Kernel, "Script terminado por linea erronea");
			free(script_en_ejecucion->lineas);
			free(script_en_ejecucion);
			break;
		}
		pthread_mutex_unlock(&exec_mutex);
	}
}

int ejecutar_quantum(Script** script, int serverSocket) {
	Script* scriptEnExec = *script;
	int entradaValida;
	int ejecutadas = 1;
	int header;
	do {
		printf("Ejecutando un quantum \n");
		//printf("%s \n", scriptEnExec->lineas[scriptEnExec->index]);

		entradaValida = 1;
		char* entrada = scriptEnExec->lineas[scriptEnExec->index];

		char* parametros;

		separarEntrada(entrada, &header, &parametros);

		entradaValida = validarParametros(header, parametros);

		if (header == ERROR) {
			log_warning(logger_Kernel, "Comando no reconocido");
			entradaValida = 0;
		}

		if (entradaValida) {
			interpretarComando(header, parametros, serverSocket);
		} else {
			return CORTE_SCRIPT_POR_LINEA_ERRONEA;
		}
		free(parametros);
		ejecutadas++;

		scriptEnExec->index++;

	} while ((scriptEnExec->index < scriptEnExec->cant_lineas)
			&& (ejecutadas <= quantum));
	if (scriptEnExec->index == scriptEnExec->cant_lineas) {
		return CORTE_SCRIPT_POR_FINALIZACION;
	}
	return CORTE_SCRIPT_POR_FIN_QUANTUM;
}

