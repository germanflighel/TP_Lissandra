/*
 ============================================================================
 Name        : LSF.c
 Author      : 
 Version     :
 Copyright   : Your copyright notice
 Description : Hello World in C, Ansi-style
 ============================================================================
 */

#include "LFS.h"

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
		hints.ai_family = AF_UNSPEC;		// No importa si uso IPv4 o IPv6
		hints.ai_flags = AI_PASSIVE;		// Asigna el address del localhost: 127.0.0.1
		hints.ai_socktype = SOCK_STREAM;	// Indica que usaremos el protocolo TCP

		getaddrinfo(NULL, PUERTO, &hints, &serverInfo); // Notar que le pasamos NULL como IP, ya que le indicamos que use localhost en AI_PASSIVE


		/*
		 * 	Descubiertos los misterios de la vida (por lo menos, para la conexion de red actual), necesito enterarme de alguna forma
		 * 	cuales son las conexiones que quieren establecer conmigo.
		 *
		 * 	Para ello, y basandome en el postulado de que en Linux TODO es un archivo, voy a utilizar... Si, un archivo!
		 *
		 * 	Mediante socket(), obtengo el File Descriptor que me proporciona el sistema (un integer identificador).
		 *
		 */
		/* Necesitamos un socket que escuche las conecciones entrantes */
		int listenningSocket;
		listenningSocket = socket(serverInfo->ai_family, serverInfo->ai_socktype, serverInfo->ai_protocol);

		/*
		 * 	Perfecto, ya tengo un archivo que puedo utilizar para analizar las conexiones entrantes. Pero... ������Por donde?
		 *
		 * 	Necesito decirle al sistema que voy a utilizar el archivo que me proporciono para escuchar las conexiones por un puerto especifico.
		 *
		 * 				OJO! Todavia no estoy escuchando las conexiones entrantes!
		 *
		 */
		bind(listenningSocket,serverInfo->ai_addr, serverInfo->ai_addrlen);
		freeaddrinfo(serverInfo); // Ya no lo vamos a necesitar

		/*
		 * 	Ya tengo un medio de comunicacion (el socket) y le dije por que "telefono" tiene que esperar las llamadas.
		 *
		 * 	Solo me queda decirle que vaya y escuche!
		 *
		 */
		listen(listenningSocket, BACKLOG);		// IMPORTANTE: listen() es una syscall BLOQUEANTE.

		printf("Esperando memoria... \n");

		/*
		 * 	El sistema esperara hasta que reciba una conexion entrante...
		 * 	...
		 * 	...
		 * 	BING!!! Nos estan llamando! ������Y ahora?
		 *
		 *	Aceptamos la conexion entrante, y creamos un nuevo socket mediante el cual nos podamos comunicar (que no es mas que un archivo).
		 *
		 *	������Por que crear un nuevo socket? Porque el anterior lo necesitamos para escuchar las conexiones entrantes. De la misma forma que
		 *	uno no puede estar hablando por telefono a la vez que esta esperando que lo llamen, un socket no se puede encargar de escuchar
		 *	las conexiones entrantes y ademas comunicarse con un cliente.
		 *
		 *			Nota: Para que el listenningSocket vuelva a esperar conexiones, necesitariamos volver a decirle que escuche, con listen();
		 *				En este ejemplo nos dedicamos unicamente a trabajar con el cliente y no escuchamos mas conexiones.
		 *
		 */
		struct sockaddr_in addr;			// Esta estructura contendra los datos de la conexion del cliente. IP, puerto, etc.
		socklen_t addrlen = sizeof(addr);

		int socketCliente = accept(listenningSocket, (struct sockaddr *) &addr, &addrlen);

		/*
		 * 	Ya estamos listos para recibir paquetes de nuestro cliente...
		 *		 *
		 *	Cuando el cliente cierra la conexion, recieve_and_deserialize() devolvera 0.
		 */

		t_Package package;
		int status = 1;		// Estructura que manjea el status de los recieve.

		//t_PackageEnv packageEnvio;
		//packageEnvio.message = malloc(MAX_MESSAGE_SIZE);
		//char *serializedPackage;

		printf("Memoria conectada. Esperando Envío de mensajes.\n");

		while (status){
			status = recieve_and_deserialize(&package, socketCliente);

			//fill_package(&packageEnvio, &username);

			// Ver el "Deserializando estructuras dinamicas" en el comentario de la funcion.
			if (status) printf("Memory says: %s", package.message);

			//serializedPackage = serializarOperandos(&packageEnvio);
			//send(socketCliente, serializedPackage, packageEnvio.total_size, 0);
			//dispose_package(&serializedPackage);
		}


		printf("Cliente Desconectado.\n");
		/*
		 * 	Terminado el intercambio de paquetes, cerramos todas las conexiones y nos vamos a mirar Game of Thrones, que seguro nos vamos a divertir mas...
		 *
		 *
		 * 																					~ Divertido es Disney ~
		 *
		 */
		close(socketCliente);
		close(listenningSocket);

		/* See ya! */

		return 0;
}

/*
typedef struct t_Package {
	uint32_t message_long;
	char* message;
	uint32_t total_size;
} t_Package;
 */

int recieve_and_deserialize(t_Package *package, int socketCliente){

	int status;
	int buffer_size;
	char *buffer = malloc(buffer_size = sizeof(uint32_t));

	uint32_t message_long;
	status = recv(socketCliente, buffer, sizeof(package->message_long), 0);
	memcpy(&(message_long), buffer, buffer_size);
	if (!status) return 0;

	package->message = malloc(message_long+1);

	status = recv(socketCliente, package->message, message_long, 0);
	if (!status) return 0;

	free(buffer);

	return status;
}

void dispose_package(char **package){
	free(*package);
}

/*

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

*/
