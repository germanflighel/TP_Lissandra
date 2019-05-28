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

int main() {

	/*
	 *
	 *  Estas y otras preguntas existenciales son resueltas getaddrinfo();
	 *
	 *  Obtiene los datos de la direccion de red y lo guarda en serverInfo.
	 *
	 */
	struct addrinfo hints;
	struct addrinfo *serverInfo;
	char* ip;
	char* puerto;

	t_list* puertos = list_create();
	list_add(puertos, 6167);
	list_add(puertos, 6168);

	logger_Kernel = iniciar_logger();

	memset(&hints, 0, sizeof(hints));
	hints.ai_family = AF_UNSPEC; // Permite que la maquina se encargue de verificar si usamos IPv4 o IPv6
	hints.ai_socktype = SOCK_STREAM;	// Indica que usaremos el protocolo TCP

	t_config *conection_conf;
	abrir_config(&conection_conf);

	ip = config_get_string_value(conection_conf, "IP");
	puerto = config_get_string_value(conection_conf, "PUERTO");
	quantum = config_get_int_value(conection_conf, "QUANTUM");

	log_info(logger_Kernel, puerto);

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

		freeaddrinfo(serverInfo);	// No lo necesitamos mas
		printf("Conectado al servidor.\n");
		log_info(logger_Kernel, "Conecte al servidor.");

	} else {
		printf("No se pudo conectar al servidor...");
		log_info(logger_Kernel, "No me pude conectar con el servidor");
	}

	printf("Esperando describe.\n");

	tablas_actuales = list_create();

	t_describe describe;
	recieve_and_deserialize_describe(&describe, serverSocket);

	for (int tabla = 0; tabla < describe.cant_tablas; tabla++) {
		Tabla tabla_nueva;
		strcpy(tabla_nueva.nombre_tabla, describe.tablas[tabla].nombre_tabla);
		tabla_nueva.consistencia = describe.tablas[tabla].consistencia;

		printf("Tabla %s \n", tabla_nueva.nombre_tabla);
		printf("Consistencia %s \n",
				consistency_to_str(tabla_nueva.consistencia));

		list_add(tablas_actuales, &tabla_nueva);
	}

	//setup planificacion
	colaReady = queue_create();
	colaExec = queue_create();
	sem_init(&ejecutar_sem, 0, 0);
	pthread_mutex_init(&cola_ready_mutex, NULL);
	//setup planificacion

	//thread executer
	pthread_t threadL;
	int iret1;

	iret1 = pthread_create(&threadL, NULL, exec, (void*) serverSocket);
	if (iret1) {
		fprintf(stderr, "Error - pthread_create() return code: %d\n", iret1);
		exit(EXIT_FAILURE);
	}
	//thread executer

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
		entradaValida = 1;
		char* entrada = leerConsola();

		char* parametros;
		int header;

		separarEntrada(entrada, &header, &parametros);

		if (header == EXIT_CONSOLE) {
			enviar = 0;
		} else if (header == ERROR) {
			printf("Comando no reconocido\n");
			entradaValida = 0;
		}

		free(entrada);

		if (enviar && entradaValida) {

			interpretarComando(header, parametros, serverSocket);
			free(parametros);
		}

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
	list_destroy(puertos);
	list_destroy(tablas_actuales);

	queue_destroy(colaReady);
	queue_destroy(colaExec);

	/* ADIO'! */
	return 0;
}

void abrir_config(t_config** g_config) {

	(*g_config) = config_create(CONFIG_PATH);

}

t_log* iniciar_logger(void) {

	return log_create(LOG_FILE_PATH, "kernel", 1, LOG_LEVEL_INFO);

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
		printf("SELECT enviado (Tabla: %s, Key: %d)\n", package.tabla,
				package.key);

		serializedPackage = serializarSelect(&package);

		send(serverSocket, serializedPackage, package.total_size, 0);

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
		printf(
				"INSERT enviado (Tabla: %s, Key: %d, Value: %s, Timestamp: %d)\n",
				package.tabla, package.key, package.value, package.timestamp);

		serializedPackage = serializarInsert(&package);

		send(serverSocket, serializedPackage, package.total_size, 0);

		free(package.tabla);
		dispose_package(&serializedPackage);
	}
}

void describe(char* parametros, int serverSocket) {
	printf("Recibi un describe.\n");
}

void drop(char* parametros, int serverSocket) {
	printf("Recibi un drop.\n");
}

void create(char* parametros, int serverSocket) {
	printf("Recibi un create.\n");
}

void journal(char* parametros, int serverSocket) {
	printf("Recibi un journal.\n");
}

void add(char* parametros, int serverSocket) {
	printf("Recibi un add.\n");
}

void metrics(char* parametros, int serverSocket) {
	printf("Recibi un metrics.\n");
}

void run(char* rutaRecibida, int serverSocket) {

	char* entrada[MAX_MESSAGE_SIZE];
	char* parametros;
	int header;
	int enviar = 1;
	int entradaValida = 1;

	char* rutaArchivo = malloc(strlen(rutaRecibida));
	strcpy(rutaArchivo, rutaRecibida);

	printf("%s\n", rutaArchivo);

	Script* test = levantar_script(rutaArchivo);

	script_a_ready(test);

	/*
	 FILE *lql;
	 lql = fopen(rutaArchivo, "r");

	 if (!is_regular_file(rutaArchivo)) {
	 printf("La ruta es un directorio\n");
	 free(rutaArchivo);
	 return;
	 }

	 if ((lql == NULL)) {
	 printf("No se puede abrir el arhivo \n");
	 free(rutaArchivo);
	 return;	//Loguear que no se abrio el archivo
	 } else {			//Se abrio el archivo y se va a leer los comandos

	 while (feof(lql) == 0) {
	 fgets(entrada, sizeof(entrada), lql);//Lee una linea del archivo de txt
	 //printf("%s \n", entrada);

	 separarEntrada(entrada, &header, &parametros);
	 //printf("%d \n",header);
	 if (header == EXIT_CONSOLE) {
	 enviar = 0;
	 return;
	 } else if (header == ERROR) {
	 printf("Comando no reconocido\n");
	 entradaValida = 0;

	 }

	 if (enviar && entradaValida) {
	 if (parametros[strlen(parametros) - 1] == '\n') {
	 parametros[strlen(parametros) - 1] = '\0';
	 }
	 interpretarComando(header, parametros, serverSocket);
	 }

	 }
	 }

	 fclose(lql);
	 */
	free(rutaArchivo);

}

Script* levantar_script(char* ruta) {

	Script* nuevoScript = malloc(sizeof(Script));
	nuevoScript->index = 0;
	char* f;
	log_info(logger_Kernel, ruta);

	int fd = open(ruta, O_RDONLY, S_IRUSR | S_IWUSR);

	struct stat s;
	int status = fstat(fd, &s);
	int size = s.st_size;

	f = mmap(NULL, size, PROT_READ, MAP_PRIVATE, fd, 0);

	nuevoScript->lineas = string_split(f, "\n");
	nuevoScript->cant_lineas = cant_parametros(nuevoScript->lineas);

	log_info(logger_Kernel, string_itoa(nuevoScript->cant_lineas));

	close(fd);
	return nuevoScript;

}

int is_regular_file(const char *path) {

	struct stat path_stat;
	stat(path, &path_stat);
	return S_ISREG(path_stat.st_mode); //Devuelve verdadero si la ruta es un archivo
}

void script_a_ready(Script* script) {

	pthread_mutex_lock(&cola_ready_mutex);
	queue_push(colaReady, (void*) script);
	pthread_mutex_unlock(&cola_ready_mutex);

	sem_post(&ejecutar_sem);

}

void* exec(int serverSocket) {
	int resultado_exec;
	while (true) {

		sem_wait(&ejecutar_sem);

		pthread_mutex_lock(&cola_ready_mutex);
		Script* script_en_ejecucion = (Script*) queue_pop(colaReady);
		pthread_mutex_unlock(&cola_ready_mutex);

		resultado_exec = ejecutar_quantum(&script_en_ejecucion, serverSocket);

		switch (resultado_exec) {
		case CORTE_SCRIPT_POR_FINALIZACION:
			log_info(logger_Kernel, "Finalizó");
			free(script_en_ejecucion->lineas);
			free(script_en_ejecucion);
			break;
		case CORTE_SCRIPT_POR_FIN_QUANTUM:
			log_info(logger_Kernel, "Finalizó quantum");
			script_a_ready(script_en_ejecucion);
			break;
		case CORTE_SCRIPT_POR_LINEA_ERRONEA:
			log_info(logger_Kernel, "Script terminado por linea erronea");
			free(script_en_ejecucion->lineas);
			free(script_en_ejecucion);
			break;
		}

	}
}

int ejecutar_quantum(Script** script, int serverSocket) {
	Script* scriptEnExec = *script;
	int entradaValida;
	int ejecutadas = 1;
	do {
		entradaValida = 1;
		char* entrada = scriptEnExec->lineas[scriptEnExec->index];

		char* parametros;
		int header;

		separarEntrada(entrada, &header, &parametros);

		if (header == ERROR) {
			printf("Comando no reconocido\n");
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

