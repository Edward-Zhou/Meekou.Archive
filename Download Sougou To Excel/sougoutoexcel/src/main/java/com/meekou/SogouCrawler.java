package com.meekou;

import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellStyle;
import org.apache.poi.ss.usermodel.CreationHelper;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

public class SogouCrawler {
    Set<IndexTrend> indexTrends = new HashSet<IndexTrend>();
    String url;
    public SogouCrawler(String url) {
        this.url = url;
    }

    public Set<IndexTrend> Crawl() throws IOException{
        Document doc = Jsoup.connect(url).get();
        String script = doc.selectFirst("script").html();
        Pattern p = Pattern.compile("root.SG.wholedata = (.*)", Pattern.MULTILINE);
        Matcher matcher = p.matcher(script);
        String wholedata = "";
        while(matcher.find()) {
            wholedata = matcher.group(1);
        }
        ObjectMapper objectMapper = new ObjectMapper();
        SougouData sougouData = objectMapper.readValue(wholedata, SougouData.class);
        for (Pv pv : sougouData.pvList.get(0)) {
            IndexTrend indexTrend = new IndexTrend();
            indexTrend.date = pv.date;
            indexTrend.PV = pv.pv;
            indexTrends.add(indexTrend);
        }
        SaveToExcel(indexTrends);
        return indexTrends;              
    }
    public void SaveToExcel(Set<IndexTrend> indexTrends) throws IOException {
        XSSFWorkbook workbook = new XSSFWorkbook();
        XSSFSheet sheet = workbook.createSheet("Pet");
        int rowNum = 0;
        for (IndexTrend indexTrend : indexTrends) {
            Row row = sheet.createRow(rowNum++);            
            Cell date = row.createCell(0);
            Cell pv = row.createCell(1);
            CellStyle cellStyle = workbook.createCellStyle();  
            CreationHelper createHelper = workbook.getCreationHelper();
            cellStyle.setDataFormat(  
                createHelper.createDataFormat().getFormat("yyyy-MM-dd"));  
            date.setCellValue(indexTrend.date);
            date.setCellStyle(cellStyle);
            pv.setCellValue(indexTrend.PV);            
        }
        FileOutputStream outputStream = new FileOutputStream("./Download Sougou To Excel/petdata.xlsx");
        workbook.write(outputStream);
        workbook.close();
    }
}