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

	/*
	 *	Estoy conectado! Ya solo me queda una cosa:
	 *
	 *	Enviar datos!
	 *
	 *	Debemos obtener el mensaje que el usuario quiere mandar al chat, y luego serializarlo.
	 *
	 *	Si el mensaje es "exit", saldremos del sistema.
	 *
	 */

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
			}

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

	void interpretarComando(int header, char* parametros) {

		switch (header) {
		case 1:
			select_kernel(parametros);
			break;
		case 2:
			insert_kernel(parametros);
			break;
		case 3:
			describe(parametros);
			break;
		case 4:
			drop(parametros);
			break;
		case 5:
			create(parametros);
			break;
		case 6:
			journal(parametros);
			break;
		case 7:
			run(parametros);
			break;
		case 8:
			add(parametros);
			break;
		case 9:
			metrics(parametros);
			break;
		case -1:
			break;
		}

	}

	void select_kernel(char* parametros) {
		printf("Recibi un select.\n");
	}

	void insert_kernel(char* parametros) {
		printf("Recibi un insert.\n");
	}

	void describe(char* parametros) {
		printf("Recibi un describe.\n");
	}

	void drop(char* parametros) {
		printf("Recibi un drop.\n");
	}

	void create(char* parametros) {
		printf("Recibi un create.\n");
	}

	void journal(char* parametros) {
		printf("Recibi un journal.\n");
	}

	void add(char* parametros) {
		printf("Recibi un add.\n");
	}

	void metrics(char* parametros) {
		printf("Recibi un metrics.\n");
	}

	void run(char* parametros) {
		printf("Recibi un run.\n");

		char* rutaArchivo = malloc(strlen(parametros));
		strcpy(rutaArchivo, parametros);
		printf("%s\n", rutaArchivo);

		FILE *lql;
		lql = fopen(rutaArchivo, "r");

		char buffer[256];

		if ((lql == NULL)) {
			//Loguear que no se abrio el archivo
		} else {			//Se abrio el archivo y se va a leer los comandos

			while (feof(lql) == 0) {
				fgets(buffer, sizeof(buffer), lql);	//Lee una linea del archivo de txt
				printf("%s\n", buffer);
			}
		}

		fclose(lql);
		free(rutaArchivo);

	}
