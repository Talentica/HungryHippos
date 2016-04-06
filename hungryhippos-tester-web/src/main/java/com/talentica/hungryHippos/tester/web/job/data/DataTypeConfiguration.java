package com.talentica.hungryHippos.tester.web.job.data;

import javax.persistence.EnumType;
import javax.persistence.Enumerated;

import com.talentica.hungryHippos.client.domain.DataLocator.DataType;

public class DataTypeConfiguration {

	private Integer columnIndex;

	private String columnName;

	@Enumerated(EnumType.STRING)
	private DataType dataType;

	public Integer getColumnIndex() {
		return columnIndex;
	}

	public void setColumnIndex(Integer columnIndex) {
		this.columnIndex = columnIndex;
	}

	public String getColumnName() {
		return columnName;
	}

	public void setColumnName(String columnName) {
		this.columnName = columnName;
	}

	public DataType getDataType() {
		return dataType;
	}

	public void setDataType(DataType dataType) {
		this.dataType = dataType;
	}

}
