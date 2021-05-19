package com.meekou;

import java.io.IOException;
import java.util.Set;

/**
 * Hello world!
 */
public final class App {
    private App() {
    }

    /**
     * Says hello to the world.
     * @param args The arguments of the program.
     * @throws IOException
     */
    public static void main(String[] args) throws IOException {
        System.out.println("Hello World!");
        String petUrl = "http://zhishu.sogou.com/index/searchHeat?kwdNamesStr=%E5%AE%A0%E7%89%A9&timePeriodType=YEAR&dataType=SEARCH_ALL&queryType=INPUT";
        SogouCrawler sogouCrawler = new SogouCrawler(petUrl);
        Set<IndexTrend> pets = sogouCrawler.Crawl();
    }
}
