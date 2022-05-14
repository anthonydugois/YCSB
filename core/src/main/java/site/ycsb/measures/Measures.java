package site.ycsb.measures;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class Measures {
	public static final Measures instance = new Measures();

	private final ConcurrentMap<String, Measure> measures = new ConcurrentHashMap<>();

	public Measure getOrCreate(String name, Measure.Type type) throws MeasureException {
		Measure measure = measures.get(name);

		if (measure == null) {
			measure = Measure.create(name, type);

			Measure prevMeasure = measures.putIfAbsent(name, measure);

			if (prevMeasure != null) {
				measure = prevMeasure;
			}
		}

		if (measure == null || measure.getType() != type) {
			throw new MeasureException();
		}

		return measure;
	}

	public void export(Exporter exporter) {
		for (Measure measure : measures.values()) {
			measure.export(exporter);
		}
	}
}
