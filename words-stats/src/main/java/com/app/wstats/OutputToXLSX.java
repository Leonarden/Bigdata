package com.app.wstats;

import java.awt.Color;
import java.io.BufferedReader;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.Map;
import java.util.Map.Entry;

import static java.util.Map.Entry.*;

import org.apache.commons.math.stat.descriptive.moment.Mean;
import org.apache.commons.math.stat.descriptive.moment.StandardDeviation;
import org.apache.poi.sl.draw.binding.CTColor;
import org.apache.poi.sl.draw.binding.CTSRgbColor;
import org.apache.poi.ss.util.CellAddress;
import org.apache.poi.xssf.usermodel.XSSFCell;
import org.apache.poi.xssf.usermodel.XSSFCellStyle;
import org.apache.poi.xssf.usermodel.XSSFColor;
import org.apache.poi.xssf.usermodel.XSSFRow;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.apache.poi.xssf.usermodel.extensions.XSSFCellBorder;





public class OutputToXLSX {

	public static void csvToXLSX() {
	    try {
	        String csvFileAddress = "test.csv"; //csv file address
	        String xlsxFileAddress = "test.xlsx"; //xlsx file address
	        XSSFWorkbook workBook = new XSSFWorkbook();
	        XSSFSheet sheet = workBook.createSheet("sheet1");
	        String currentLine=null;
	        int RowNum=0;
	        BufferedReader br = new BufferedReader(new FileReader(csvFileAddress));
	        while ((currentLine = br.readLine()) != null) {
	            String str[] = currentLine.split(",");
	            RowNum++;
	            XSSFRow currentRow=sheet.createRow(RowNum);
	            for(int i=0;i<str.length;i++){
	                currentRow.createCell(i).setCellValue(str[i]);
	            }
	        }

	        FileOutputStream fileOutputStream =  new FileOutputStream(xlsxFileAddress);
	        workBook.write(fileOutputStream);
	        fileOutputStream.close();
	        System.out.println("Done");
	    } catch (Exception ex) {
	        System.out.println(ex.getMessage()+"Exception in try");
	    }
	}
	
	public static XSSFWorkbook createWorkbook(String filename,int sheets,int rows,int cells) {

	    
        XSSFWorkbook workBook = new XSSFWorkbook();
        try {
        int RowNum=0;
        
        for(int s=0;s<sheets;s++) {
        XSSFSheet sheet = workBook.createSheet("sheet"+s);
        
        
        for(int r=0;r<rows;r++) {
            	XSSFRow currentRow=sheet.createRow(r);
            	for(int c=0;c<cells;c++){
            		currentRow.createCell(c).setCellValue("");
            	}
        	}
        }
        FileOutputStream fileOutputStream =  new FileOutputStream(filename);
        workBook.write(fileOutputStream);
        fileOutputStream.close();
        System.out.println("Done");
        } catch (Exception ex) {
        System.out.println(ex.getMessage()+" Exception in createbook");
    }

        return workBook;
	}
	public static void writeBook(XSSFWorkbook workBook,String filename, SortedMap<String,List<RowColumn>> map, List<WordData> wordDataList) {
		String word = null;
		List<RowColumn> rscolumns = null;
		int numColored = 5;
		int maxRow = -1;
		int currRow = -1;
		Color[] colors = { Color.GREEN,Color.YELLOW,Color.GRAY,Color.BLACK,Color.RED };
		try {
			XSSFSheet sheet = workBook.getSheetAt(0);
		    XSSFRow row = null; 
			XSSFCell cell = null;
		     CellAddress caddress = null;
		     
		    /* CTColor c = new CTColor();
		     c.setSrgbClr(new CTSRgbColor().s);
		     workBook.getStylesSource().getCTStylesheet().addNewColors();
		    */
		     
		     Collections.sort(wordDataList,Collections.reverseOrder());
		     
		     for(WordData  wf: wordDataList) {
		    	 word = wf.getWord();
		    	 rscolumns = map.get(word);
		    	 Collections.sort(rscolumns);
		    	 for(RowColumn rc: rscolumns) {
		    		 //caddress = new CellAddress(rc.getRow(),rc.getColumn());
		    		 //sheet.setActiveCell(caddress);
		    		 currRow = rc.getRow();
		    		 if(currRow>maxRow)
		    			 maxRow = currRow;
		    		 row = sheet.getRow(currRow);
		    		 cell = row.getCell(rc.getColumn());
		    		 if(cell ==null)
		    			 cell = row.createCell(rc.getColumn());
		    		
		    		 cell.setCellValue(word);
		    		 if(numColored>0) {
		    			 XSSFCellStyle style = cell.getCellStyle();
		    			 style.setFillForegroundColor( new XSSFColor(colors[numColored-1]));
		    			 cell.setCellStyle(style);
		    		 }
		    	 
		    	 }
		    	 numColored--;
		     }
			
			FileOutputStream fileOutputStream =  new FileOutputStream(filename);
	        workBook.write(fileOutputStream);
	        fileOutputStream.close();
	        
	        
	        writeBookStatistics(workBook,filename,0,maxRow,map,wordDataList);
	        		
	               
	        System.out.println("Book Written");
	     
	        
	        
		}catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	
	public static void writeBookStatistics(XSSFWorkbook workBook,String filename,int currSheet,int maxRow, SortedMap<String,List<RowColumn>> map, List<WordData> wordDataList) throws Exception {
		int idx=0;
		int rindex = maxRow;
		int rindexStart = 0;
		XSSFSheet sheet = null;
		double[] freqs = new double[wordDataList.size()];
		double[] charfreqs = null;
		SortedMap<Character,Integer> mostRepeatedCharMap = null;
		StandardDeviation sd = null;
		Mean mean = null;
		
		try {
			
			sheet = workBook.getSheetAt(currSheet);
			rindex++;
			XSSFRow row = sheet.getRow(rindex);
			if(row == null)
				row = sheet.createRow(rindex);
			XSSFRow rowStart = null;
			
			//
			int cellIdx = 0;
			XSSFCell curCell = row.getCell(cellIdx);
			if(curCell==null)
				curCell = row.createCell(cellIdx);
			curCell.setCellValue("Text Statistics:");
			rindex++;
			row = sheet.getRow(rindex);
			if(row == null)
				row = sheet.createRow(rindex);
			rowStart = row;
			rindexStart = rindex;
			mostRepeatedCharMap = new TreeMap<Character,Integer>();
			WordData wf = null;
			Character currentChar = null;
			Integer sum = 0;
			for(int i=0;i<wordDataList.size();i++) {
				wf = wordDataList.get(i);
				rindex++; cellIdx = 0;
				row = sheet.getRow(rindex);
				if(row == null)
					row = sheet.createRow(rindex);
				curCell = row.getCell(cellIdx);
				if(curCell==null)
					curCell = row.createCell(cellIdx);
				curCell.setCellValue(wf.getWord());
				cellIdx++;
				curCell = row.getCell(cellIdx);
				if(curCell==null)
					curCell = row.createCell(cellIdx);
				curCell.setCellValue(wf.getFrequency());
				freqs[idx]= Integer.valueOf(wf.getFrequency()).doubleValue();
				idx++;
				currentChar = wf.getMostRepeatedChar();
				sum = mostRepeatedCharMap.get(currentChar);
				if(sum==null)
					sum = wf.getMostRepeatedCharFreq();
				else 
					sum = sum + wf.getMostRepeatedCharFreq();
				mostRepeatedCharMap.put(currentChar,sum);
					
			}
			

			row = rowStart;
			cellIdx = 2;
			curCell = row.getCell(cellIdx);
			if(curCell==null)
				curCell = row.createCell(cellIdx);
			curCell.setCellValue("Mean of word frequencies");
            cellIdx++;		
			curCell = row.getCell(cellIdx);
			if(curCell==null)
				curCell = row.createCell(cellIdx);
			curCell.setCellValue("Standard deviation of word frequencies");
            rindex = rindexStart +1;
            row = sheet.getRow(rindex);
			if(row==null)
				row = sheet.createRow(rindex);
			cellIdx = 2;
			curCell = row.getCell(cellIdx);
			if(curCell==null)
				curCell = row.createCell(cellIdx);
			mean = new Mean();
			double m =	mean.evaluate(freqs);			
			curCell.setCellValue(m);
            cellIdx++;		
			curCell = row.getCell(cellIdx);
			if(curCell==null)
				curCell = row.createCell(cellIdx);
			sd = new StandardDeviation();
			double s = sd.evaluate(freqs);
			curCell.setCellValue(s);
            
          Map<Character, Integer> sorted = mostRepeatedCharMap.entrySet()
				        .stream()
				        .sorted(comparingByValue())
				        .collect(
				            Collectors.toMap(e -> e.getKey(), e -> e.getValue(), (e1, e2) -> e2,
				                LinkedHashMap<Character, Integer>::new));
			cellIdx = 4;
			row = rowStart;
			curCell = row.getCell(cellIdx);
			if(curCell==null)
				curCell = row.createCell(cellIdx);
			curCell.setCellValue("Characters Statistics");
			rindex = rindexStart +1;
			charfreqs = new double[sorted.size()];
			int k=0;
			for(Entry<Character,Integer> e:sorted.entrySet()) {
			 row = sheet.getRow(rindex);
			 if(row==null)
				row = sheet.createRow(rindex);
			 curCell = row.getCell(cellIdx);
			 if(curCell==null)
				 curCell = row.createCell(cellIdx);
			 curCell.setCellValue("" + e.getKey());
			 cellIdx++;
			 curCell = row.getCell(cellIdx);
			 if(curCell==null)
				 curCell = row.createCell(cellIdx);
			 curCell.setCellValue(e.getValue());
			 charfreqs[k++] = e.getValue().doubleValue();
			 cellIdx--;
			 rindex++;
			}
			
			row = rowStart;
			cellIdx = 6;
			curCell = row.getCell(cellIdx);
			if(curCell==null)
				curCell = row.createCell(cellIdx);
			curCell.setCellValue("Mean of char frequencies");
            cellIdx++;		
			curCell = row.getCell(cellIdx);
			if(curCell==null)
				curCell = row.createCell(cellIdx);
			curCell.setCellValue("Standard deviation of char frequencies");
            rindex = rindexStart +2;
            row = sheet.getRow(rindex);
			if(row==null)
				row = sheet.createRow(rindex);
			cellIdx = 6;
			curCell = row.getCell(cellIdx);
			if(curCell==null)
				curCell = row.createCell(cellIdx);
			 m =	mean.evaluate(charfreqs);			
			curCell.setCellValue(m);
            cellIdx++;		
			curCell = row.getCell(cellIdx);
			if(curCell==null)
				curCell = row.createCell(cellIdx);
			 s = sd.evaluate(charfreqs);
			curCell.setCellValue(s);
          
			
			
			FileOutputStream fileOutputStream =  new FileOutputStream(filename);
	        workBook.write(fileOutputStream);
	        fileOutputStream.close();
	    
			System.out.println("Statistics generated");
		}catch(Exception ex) {
			ex.printStackTrace();
			throw new Exception("Exception generating Statistics");
		}
	}
	
	
	
	public static void main(String[] args) {
		String fname = "/home/david/.wrk/user/dev/workspaces/5.0/worksbigd/words-stats/words-stats/src/main/java/resources/words.xlsx";

		createWorkbook(fname,1,10,10);
		
		
	}

}
