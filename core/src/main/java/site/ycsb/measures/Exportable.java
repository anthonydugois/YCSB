package site.ycsb.measures;

import java.io.IOException;

public interface Exportable {
	void export(Exporter exporter) throws IOException;
}
