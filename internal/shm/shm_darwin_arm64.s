#include "textflag.h"

TEXT libc_shm_open_trampoline<>(SB),NOSPLIT,$0-0
	JMP	libc_shm_open(SB)
GLOBL	·libc_shm_open_trampoline_addr(SB), RODATA, $8
DATA	·libc_shm_open_trampoline_addr(SB)/8, $libc_shm_open_trampoline<>(SB)

TEXT libc_shm_unlink_trampoline<>(SB),NOSPLIT,$0-0
	JMP	libc_shm_unlink(SB)
GLOBL	·libc_shm_unlink_trampoline_addr(SB), RODATA, $8
DATA	·libc_shm_unlink_trampoline_addr(SB)/8, $libc_shm_unlink_trampoline<>(SB)
