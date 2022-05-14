package site.ycsb.measures;

import java.io.*;

public abstract class Exporter implements Closeable {
	private final OutputStream outputStream;

	public Exporter(OutputStream outputStream) {
		this.outputStream = outputStream;
	}

	public abstract void write(String str) throws IOException;

	public static class ConsoleExporter extends Exporter {
		private final BufferedWriter buffer;

		public ConsoleExporter(OutputStream outputStream) {
			super(outputStream);

			this.buffer = new BufferedWriter(new OutputStreamWriter(outputStream));
		}

		@Override
		public void write(String str) throws IOException {
			buffer.write(str);
			buffer.newLine();
		}

		@Override
		public void close() throws IOException {
			buffer.close();
		}
	}
}
