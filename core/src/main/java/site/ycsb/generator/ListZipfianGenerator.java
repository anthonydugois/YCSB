package site.ycsb.generator;

import java.util.List;

public class ListZipfianGenerator extends ZipfianGenerator {
	private final List<Long> itemList;

	public ListZipfianGenerator(List<Long> itemList, double zipfianConstant) {
		super(itemList.size(), zipfianConstant);

		this.itemList = itemList;
	}

	@Override
	public Long nextValue() {
		int index = Math.toIntExact(super.nextValue());

		return itemList.get(index);
	}
}
