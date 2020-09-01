package com.app.wstats;

public class RowColumn implements Comparable<RowColumn> {
	private Integer row = 0;
	private Integer column = 0;
	
	
	public RowColumn(Integer row, Integer column) {
		this.row = row;
		this.column = column;
	}
	

	public Integer getRow() {
		return row;
	}





	public void setRow(Integer row) {
		this.row = row;
	}





	public Integer getColumn() {
		return column;
	}





	public void setColumn(Integer column) {
		this.column = column;
	}





	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return this.row+";"+column;
	}





	public int compareTo(RowColumn o) {
		// TODO Auto-generated method stub
		
		return this.row.compareTo(o.row)+ this.column.compareTo(o.column);
	
	}
	
	
	
	
	

}
