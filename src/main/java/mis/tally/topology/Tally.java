/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package mis.tally.topology;

import java.io.Serializable;
import java.util.List;

/**
 *
 * @author jwalton
 */
public class Tally implements Serializable{
    
    private List<String> tallyFields;
    private String query;
    private String name;
    
    public Tally(String name, String query, List<String> tallyFields){
        this.name = name;
        this.query = query;
        this.tallyFields = tallyFields;
    }

    public List<String> getTallyFields() {
        return tallyFields;
    }

    public void setTallyFields(List<String> tallyFields) {
        this.tallyFields = tallyFields;
    }

    public String getQuery() {
        return query;
    }

    public void setQuery(String query) {
        this.query = query;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
    
    
}
