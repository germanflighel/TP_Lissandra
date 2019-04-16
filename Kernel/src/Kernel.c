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

		memset(&hints, 0, sizeof(hints));
		hints.ai_family = AF_UNSPEC;		// Permite que la maquina se encargue de verificar si usamos IPv4 o IPv6
		hints.ai_socktype = SOCK_STREAM;	// Indica que usaremos el protocolo TCP

		getaddrinfo(IP, PUERTO, &hints, &serverInfo);	// Carga en serverInfo los datos de la conexion


		/*
		 * 	Ya se quien y a donde me tengo que conectar... ������Y ahora?
		 *	Tengo que encontrar una forma por la que conectarme al server... Ya se! Un socket!
		 *
		 * 	Obtiene un socket (un file descriptor -todo en linux es un archivo-), utilizando la estructura serverInfo que generamos antes.
		 *
		 */
		int serverSocket;
		serverSocket = socket(serverInfo->ai_family, serverInfo->ai_socktype, serverInfo->ai_protocol);

		/*
		 * 	Perfecto, ya tengo el medio para conectarme (el archivo), y ya se lo pedi al sistema.
		 * 	Ahora me conecto!
		 *
		 */
		if(connect(serverSocket, serverInfo->ai_addr, serverInfo->ai_addrlen)==0){
			freeaddrinfo(serverInfo);	// No lo necesitamos mas
			printf("Conectado al servidor.\n");
		}else{
			printf("No se pudo conectar al servidor...");
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
		t_Package package;
		package.message = malloc(MAX_MESSAGE_SIZE);
		char *serializedPackage;

		//t_PackageRec packageRec;
		//int status = 1;		// Estructura que manjea el status de los recieve.


		printf("Bienvenido al sistema, puede comenzar a escribir. Escriba 'exit' para salir.\n");

		while(enviar){

			fill_package(&package); // Completamos el package, que contendra los datos del mensaje que vamos a enviar.

			if(!strcmp(package.message, "exit\n")){
				enviar = 0;
			} 		// Chequeamos si el usuario quiere salir.

			if(enviar) {
				serializedPackage = serializarOperandos(&package);	// Ver: ������Por que serializacion dinamica? En el comentario de la definicion de la funcion.
				send(serverSocket, serializedPackage, package.total_size, 0);
				dispose_package(&serializedPackage);

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

		/* ADIO'! */
		return 0;
}

void fill_package(t_Package *package){
	/* Me guardo el mensaje que manda */

	fgets(package->message, MAX_MESSAGE_SIZE, stdin);
	(package->message)[strlen(package->message)] = '\0';

	package->message_long = strlen(package->message) + 1;	// Me guardo lugar para el \0

	package->total_size = sizeof(package->message_long) + package->message_long;
	// Si, este ultimo valor es calculable. Pero a fines didacticos la calculo aca y la guardo a futuro, ya que no se modificara en otro lado.
}

char* serializarOperandos(t_Package *package){

	char *serializedPackage = malloc(package->total_size);

	int offset = 0;
	int size_to_send;

	size_to_send =  sizeof(package->message_long);
	memcpy(serializedPackage + offset, &(package->message_long), size_to_send);
	offset += size_to_send;

	size_to_send =  package->message_long;
	memcpy(serializedPackage + offset, package->message, size_to_send);

	return serializedPackage;
}

void dispose_package(char **package){
	free(*package);
}
