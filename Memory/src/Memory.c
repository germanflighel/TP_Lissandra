/*
 ============================================================================
 Name        : Memory.c
 Author      : 
 Version     :
 Copyright   : Your copyright notice
 Description : Hello World in C, Ansi-style
 ============================================================================
 */

#include "Memory.h"
#include "sockets.h"

int main() {

			struct addrinfo hints;
			struct addrinfo *serverInfo;
			char* ip;
			char* puerto;

			memset(&hints, 0, sizeof(hints));
			hints.ai_family = AF_UNSPEC;		// Permite que la maquina se encargue de verificar si usamos IPv4 o IPv6
			hints.ai_socktype = SOCK_STREAM;	// Indica que usaremos el protocolo TCP

			abrir_log();

			t_config *conection_conf;
			abrir_con(&conection_conf);

			ip = config_get_string_value(conection_conf, "IP");
			puerto = config_get_string_value(conection_conf, "PUERTO_DEST");




			getaddrinfo(ip, puerto, &hints, &serverInfo);	// Carga en serverInfo los datos de la conexion

			config_destroy(conection_conf);

			/*
			 * 	Ya se quien y a donde me tengo que conectar... ������Y ahora?
			 *	Tengo que encontrar una forma por la que conectarme al server... Ya se! Un socket!
			 *
			 * 	Obtiene un socket (un file descriptor -todo en linux es un archivo-), utilizando la estructura serverInfo que generamos antes.
			 *
			 */
			int lfsSocket;
			lfsSocket = socket(serverInfo->ai_family, serverInfo->ai_socktype, serverInfo->ai_protocol);

			/*
			 * 	Perfecto, ya tengo el medio para conectarme (el archivo), y ya se lo pedi al sistema.
			 * 	Ahora me conecto!
			 *
			 */
			connect(lfsSocket, serverInfo->ai_addr, serverInfo->ai_addrlen);
			freeaddrinfo(serverInfo);	// No lo necesitamos mas

			printf("Conectado al LFS \n");
	/*

		 *
		 *  Estas y otras preguntas existenciales son resueltas getaddrinfo();
		 *
		 *  Obtiene los datos de la direccion de red y lo guarda en serverInfo.
		 *
		 */

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

		int waiting = 1;
		while(waiting){
			wait(1000);
			if(bind(listenningSocket,serverInfo->ai_addr, serverInfo->ai_addrlen)!= -1) {
			         waiting = 0;
			}
		}

		freeaddrinfo(serverInfo); // Ya no lo vamos a necesitar

		/*
		 * 	Ya tengo un medio de comunicacion (el socket) y le dije por que "telefono" tiene que esperar las llamadas.
		 *
		 * 	Solo me queda decirle que vaya y escuche!
		 *
		 */
		listen(listenningSocket, BACKLOG);		// IMPORTANTE: listen() es una syscall BLOQUEANTE.

		printf("Esperando kernel... \n");

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

		t_PackagePosta package;
		int status = 1;		// Estructura que manjea el status de los recieve.

		char *serializedPackage;


		printf("Cliente conectado. Esperando Envío de mensajessss.\n");

		while (status){
			status = recieve_and_deserialize(&package, socketCliente);

			//fill_package(&packageEnvio, &username);

			// Ver el "Deserializando estructuras dinamicas" en el comentario de la funcion.
			if (status) {

			char *mensaje = malloc(package.message_long);

			memcpy(mensaje,package.message,package.message_long);

			printf("%s\n",mensaje);
			log_info(g_logger, mensaje);

			serializedPackage = serializarOperandos(&package);
			send(lfsSocket, serializedPackage, package.total_size, 0);

			dispose_package(&serializedPackage);
			}

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

		log_destroy(g_logger);

		/* See ya! */

		return 0;
}



void abrir_con(t_config** g_config)
{

	(*g_config) = config_create(CONFIG_PATH);

}

void abrir_log(void)
{

	g_logger = log_create("memory_global.log", "memory", 0, LOG_LEVEL_INFO);

}


/*
typedef struct t_Package {
	uint32_t message_long;
	char* message;
	uint32_t total_size;
} t_Package;
 */
