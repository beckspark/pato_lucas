"""Encuentra un puerto TCP libre a partir de un puerto base."""

import socket
import sys


def encontrar_puerto_libre(puerto_base: int, intentos: int = 100) -> int:
    for puerto in range(puerto_base, puerto_base + intentos):
        try:
            s = socket.socket()
            s.bind(("", puerto))
            s.close()
            return puerto
        except OSError:
            continue
    return puerto_base  # fallback al puerto base si no encuentra uno libre


if __name__ == "__main__":
    puerto_base = int(sys.argv[1]) if len(sys.argv) > 1 else 8080
    print(encontrar_puerto_libre(puerto_base))
