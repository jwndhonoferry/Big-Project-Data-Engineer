package Crawl_inform;

import java.io.IOException;


import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
//import scala.util.parsing.json.JSONObject;
import org.json.JSONObject;

public class crawl_url {

    String getMetaTag(Document document, String attr) {
        Elements elementss = document.select("meta[property=" + attr + "]");
        for (Element element : elementss) {
            final String s = element.attr("content");
            if (s != null) return s;
        }
        return null;
    }
    public JSONObject get_information_spotify(String url) {
        crawl_url getData = new crawl_url();
        JSONObject data = new JSONObject();
        try {
            Document page = (Document) Jsoup.connect(url).get();
            String full_title = getData.getMetaTag(page, "og:title");
            String title = getData.getMetaTag(page, "twitter:title");
            String description = getData.getMetaTag(page, "og:description");
            String artist = getData.getMetaTag(page, "twitter:audio:artist_name");
            String type = getData.getMetaTag(page, "og:type");
            String twitter_desc = getData.getMetaTag(page, "twitter:description");

            data.put("title", title);
            data.put("full_title", full_title);
            data.put("description", description);
            data.put("artist", artist);
            data.put("type", type);

        }catch (IOException e){
            System.out.println(e);
            e.printStackTrace();
        }
        return data;
    }

//	public static void main(String[] args) {
//		crawl_url cr = new crawl_url();
////		cr.get_information_spotify("https:\\/\\/open.spotify.com\\/track\\/5sCwshjdYivIeXRwhVdgDw?si=SkFj2LGeR9GduBiMrj0pjA");
//		JSONObject dt = cr.get_information_spotify("https://open.spotify.com/track/5Y5dlBMUM5rTrpNo445yUC?si=VPdBjk3IRt2FuYlXd3ufMA");
//		System.out.println(dt);
//    }
}