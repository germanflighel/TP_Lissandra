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

	t_log* logger_Kernel = iniciar_logger();

	memset(&hints, 0, sizeof(hints));
	hints.ai_family = AF_UNSPEC; // Permite que la maquina se encargue de verificar si usamos IPv4 o IPv6
	hints.ai_socktype = SOCK_STREAM;	// Indica que usaremos el protocolo TCP

	t_config *conection_conf;
	abrir_config(&conection_conf);

	ip = config_get_string_value(conection_conf, "IP");
	puerto = config_get_string_value(conection_conf, "PUERTO");

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

	t_describe describe;
	recieve_and_deserialize_describe(&describe,serverSocket);

	printf("Tabla %s \n",describe.tablas[0].nombre_tabla);
	printf("Consistencia %d \n",describe.tablas[0].consistencia);

	printf("Tabla %s \n",describe.tablas[1].nombre_tabla);
	printf("Consistencia %d \n",describe.tablas[1].consistencia);

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
		/*

		 int ingresoCorrecto = 1;
		 fill_package(&package); // Completamos el package, que contendra los datos del mensaje que vamos a enviar.



		 if(package.header == ERROR){
		 ingresoCorrecto = 0;
		 printf("Comando no reconocido\n");
		 } 		// Chequeamos si el usuario quiere salir.

		 if(package.header == -1){
		 enviar = 0;
		 } 		// Chequeamos si el usuario quiere salir.

		 */

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
			/*
			 if (header == SELECT) {
			 t_PackageSelect package;
			 if (!fill_package_select(&package, parametros)) {
			 printf("Incorrecta cantidad de parametros\n");
			 entradaValida = 0;
			 }

			 if (entradaValida) {
			 printf("SELECT enviado (Tabla: %s, Key: %d)\n",
			 package.tabla, package.key);

			 serializedPackage = serializarSelect(&package);

			 send(serverSocket, serializedPackage, package.total_size,
			 0);

			 free(package.tabla);
			 dispose_package(&serializedPackage);
			 }
			 } else if (header == INSERT) {
			 t_PackageInsert package;
			 if (!fill_package_insert(&package, parametros,0)) {
			 printf("Incorrecta cantidad de parametros\n");
			 entradaValida = 0;
			 }

			 if (entradaValida) {
			 printf("INSERT enviado (Tabla: %s, Key: %d, Value: %s, Timestamp: %d)\n", package.tabla,
			 package.key,package.value,package.timestamp);

			 serializedPackage = serializarInsert(&package);

			 send(serverSocket, serializedPackage, package.total_size,
			 0);

			 free(package.tabla);
			 dispose_package(&serializedPackage);
			 }
			 }*/

			/*
			 interpretarComando(package.header,package.message);
			 serializedPackage = serializarOperandos(&package);	// Ver: ������Por que serializacion dinamica? En el comentario de la definicion de la funcion.
			 send(serverSocket, serializedPackage, package.total_size, 0);
			 dispose_package(&serializedPackage);
			 */
			//status = recieve_and_deserialize(&packageRec, serverSocket);
			//if (status) printf("%s says: %s", packageRec.username, packageRec.message);
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
	 *	...Pero me aburri. Era de esperarse, ������No?
	 *
	 *	Asique ahora solo me queda cerrar la conexion con un close();
	 */

	close(serverSocket);
	log_destroy(logger_Kernel);

	/* ADIO'! */
	return 0;
}

void abrir_config(t_config** g_config) {

	(*g_config) = config_create(CONFIG_PATH);

}

t_log* iniciar_logger(void) {

	return log_create(LOG_FILE_PATH, "kernel", 0, LOG_LEVEL_INFO);

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

		if (cant_parametros(string_split(parametros," ")) != 1){
				printf("Cantidad de parametros invalidos\n");
		}
		else{
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

	FILE *lql;
	lql = fopen(rutaArchivo, "r");

	if(!is_regular_file(rutaArchivo)){
		printf("La ruta es un directorio\n");
		free(rutaArchivo);
		return;
	}

	if ((lql == NULL)) {
		printf("No se puede abrir el arhivo \n");
		free(rutaArchivo);
		return;//Loguear que no se abrio el archivo
	}
	else {			//Se abrio el archivo y se va a leer los comandos

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
				interpretarComando(header, parametros, serverSocket);
			}

		}
	}

	fclose(lql);
	free(rutaArchivo);

}

int is_regular_file(const char *path){

	struct stat path_stat;
	stat(path,&path_stat);
	return S_ISREG(path_stat.st_mode); //Devuelve verdadero si la ruta es un archivo
}

