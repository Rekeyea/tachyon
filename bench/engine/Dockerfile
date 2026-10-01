# Imagen del motor: filesystem del host (el binario está linkado contra la
# glibc del host) + nsswitch.conf mínimo. El binario se monta desde el
# volumen tachyon-target (ver bench/build-engine.sh).
FROM tachyon-hostfs
COPY nsswitch.conf /etc/nsswitch.conf
