namespace java com.vibe


struct carrier {
  1: string carrierCode;
  2: string carrierName;
  3: string ts;
}

struct aeropuerto {
  1: string aeroCode;
  2: string aeroName;
  3: string ts;
}

struct aeropuerto_carrier {
  1: string aeroCode;
  2: string carrierCode;
  3: string ts;
}

struct estadisticas {
 1: string aeroCode;
 2: string carrierCode;
 3: string estadisticaCode;
 4: string ts;
}

struct estadisticas_cancelado {
 1: string estadisticaCode;
 2: string cancelado;
 3: string ts;
}



