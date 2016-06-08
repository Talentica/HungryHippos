/**
 * 
 */
package com.talentica.hungryHippos.client.validator;

/**
 * @author pooshans
 *
 */
public class CsvParserValidator implements CsvValidator{
	
	 private char separator ;
	 private char quotechar ;
	 private char escapechar ;;
	 private boolean isTrimWhiteSpace ;
	 private boolean isRetainOuterQuotes ;
	 
	public CsvParserValidator() {
		this.separator = DEFAULT_SEPARATOR;
		this.quotechar = DEFAULT_QUOTE_CHAR;
		this.escapechar = DEFAULT_ESCAPE_CHAR;
		this.isTrimWhiteSpace = DEFAULT_TRIM_WS;
		this.isRetainOuterQuotes = isRetainOuterQuotes;
	}

	public CsvParserValidator(final char separator, final char quotechar,
			final char escapechar, final boolean isTrimWhiteSpace,
			final boolean isRetainOuterQuotes) {
		this.separator = separator;
		this.quotechar = quotechar;
		this.escapechar = escapechar;
		this.isTrimWhiteSpace = isTrimWhiteSpace;
		this.isRetainOuterQuotes = isRetainOuterQuotes;
	}
	
	public char getSeparator() {
		return separator;
	}

	public void setSeparator(char separator) {
		this.separator = separator;
	}

	public char getQuotechar() {
		return quotechar;
	}

	public void setQuotechar(char quotechar) {
		this.quotechar = quotechar;
	}

	public char getEscapechar() {
		return escapechar;
	}

	public void setEscapechar(char escapechar) {
		this.escapechar = escapechar;
	}

	public boolean isTrimWhiteSpace() {
		return isTrimWhiteSpace;
	}

	public void setTrimWhiteSpace(boolean isTrimWhiteSpace) {
		this.isTrimWhiteSpace = isTrimWhiteSpace;
	}

	public boolean isRetainOuterQuotes() {
		return isRetainOuterQuotes;
	}

	public void setRetainOuterQuotes(boolean isRetainOuterQuotes) {
		this.isRetainOuterQuotes = isRetainOuterQuotes;
	}

	public boolean isQuoteChar(char character) {
		return (character == quotechar);
	}

	public boolean isSeparator(char character) {
		return (character == separator);
	}

	public boolean isEscapechar(char character) {
		return (character == escapechar);
	}

}
