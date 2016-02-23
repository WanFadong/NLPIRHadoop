package segment.mongoDB;

import com.sun.jna.Native;

public class SegmentHelp {
	private static CLibrary lib;

	public static CLibrary getLib() {
		if (lib == null) {
			lib = (CLibrary) Native.loadLibrary("/home/fadongwan/libNLPIR.so", CLibrary.class);
			lib.NLPIR_Init("", 1, "0");
		}
		return lib;
	}

	public static void destroyLib() {
		lib.NLPIR_Exit();
	}
}
