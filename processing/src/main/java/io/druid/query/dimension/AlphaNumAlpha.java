package io.druid.query.dimension;

import java.util.ArrayList;
import java.util.List;

public class AlphaNumAlpha {

	private String str;

	public AlphaNumAlpha(String str) {
		this.str = str;
	}

	public List<Comparable> getComparators() {

		List<Comparable> ret = new ArrayList<>();

		Type preType = null;
		int preIndex = -1;

		for (int i = 0; i < str.length(); ++i) {
			int c = str.charAt(i);

			if (preType == null) {
				if (Character.isAlphabetic(c)) {
					preType = Type.Alpha;
				} else if (Character.isDigit(c)) {
					preType = Type.Digit;
				} else {
					preType = Type.other;
				}
				preIndex = i;
			} else if (preType == Type.Alpha) {
				if (Character.isAlphabetic(c)) {

				} else if (Character.isDigit(c)) {
					ret.add(str.substring(preIndex, i));
					preType = Type.Digit;
					preIndex = i;
				} else {
					ret.add(str.substring(preIndex, i));
					preType = Type.other;
					preIndex = i;
				}

			} else if (preType == Type.Digit) {
				if (Character.isAlphabetic(c)) {
					ret.add(Integer.parseInt(str.substring(preIndex, i)));
					preType = Type.Alpha;
					preIndex = i;
				} else if (Character.isDigit(c)) {
				} else {
					ret.add(Integer.parseInt(str.substring(preIndex, i)));
					preType = Type.other;
					preIndex = i;
				}
			} else if (preType == Type.other) {
				if (Character.isAlphabetic(c)) {
					ret.add(str.substring(preIndex, i));
					preType = Type.Alpha;
					preIndex = i;
				} else if (Character.isDigit(c)) {
					ret.add(str.substring(preIndex, i));
					preType = Type.Digit;
					preIndex = i;
				} else {

				}

			} else {
				throw new RuntimeException("unknow type");
			}
		}

		if (preType == Type.Alpha || preType == Type.other) {
			ret.add(str.substring(preIndex, str.length()));
		} else if (preType == Type.Digit) {
			ret.add(Integer.parseInt(str.substring(preIndex, str.length())));
		} else {
			throw new RuntimeException("unknow type");
		}
		return ret;
	}

	enum Type {
		Alpha, Digit, other
	}
}
