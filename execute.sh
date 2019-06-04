#!/bin/sh

#  Variables

DIR1=tp-2019-1c-Ckere/
MEMORIA=Memory/Debug
KERNEL=Kernel/Debug
FILESYSTEM=LSF/Debug


#  Funciones

imprimir(){
echo "$1"
echo 
}
limpiar(){
 cd $DIR1
 (cd $MEMORIA && make clean)
    imprimir "Memoria limpia"
 (cd $KERNEL && make clean)
    imprimir "Kernel limpio"
 (cd $FILESYSTEM && make clean)
 imprimir "FileSystem limpio"

}

#  Script

imprimir "Bienvenidos SOcorro"
imprimir "Bajo el repositorio de las commons"

#(git clone https://github.com/sisoputnfrba/so-commons-library.git)
imprimir "Instalando las commonds"

#(cd so-commons-library && sudo make install)
imprimir "commons instaladas"

imprimir "Bajo el repositorio de SOcorro"
#(git clone https://github.com/sisoputnfrba/tp-2019-1c-SOcorro.git)

limpiar

imprimir "Compilando la Memoria"
#(cd $MEMORIA && make all)
imprimir "Memoria listo"

imprimir "Compilando el Kernel"
#(cd $KERNEL && make all)
imprimir "Kernel listo"

imprimir "Compilando el FileSystem"
#(cd $FILESYSTEM && make all)
imprimir "FileSystem listo"

imprimir "Modificar las configuraciones de los archivos (comando nano {file})"
