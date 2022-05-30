package site.ycsb.generator;

import java.util.List;

public class ListZipfianGenerator extends NumberGenerator {
	private final List<Long> itemList;

	private ZipfianGenerator generator;

	public ListZipfianGenerator(List<Long> itemList, double zipfianConstant) {
		this.itemList = itemList;
		this.generator = new ZipfianGenerator(itemList.size(), zipfianConstant);
	}

	@Override
	public Long nextValue() {
		int index = Math.toIntExact(generator.nextValue());

		return itemList.get(index);
	}

	@Override
	public double mean() {
		throw new UnsupportedOperationException("@todo implement mean()");
	}
}
