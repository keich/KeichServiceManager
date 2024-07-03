package ru.keich.mon.query;


public enum Operator {
	NE, EQ, LT, GT, CO, NC, ERROR, ALL;
	
	public static Operator fromString(String str) {
        switch(str.toUpperCase()) {
        case "NE":
            return NE;
        case "EQ":
            return EQ;
        case "LT":
            return LT;
        case "GT":
            return GT;
        case "CO":
            return CO;
        case "NC":
            return NC;
        }
        return ERROR;
	}
}
