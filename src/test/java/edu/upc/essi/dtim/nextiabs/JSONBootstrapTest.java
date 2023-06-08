package edu.upc.essi.dtim.nextiabs;

import edu.upc.essi.dtim.nextiabs.utils.Graph;
import org.apache.commons.compress.utils.Lists;
import org.apache.jena.riot.Lang;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Named;
import edu.upc.essi.dtim.nextiabs.JSONBootstrap;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;

import static org.junit.jupiter.api.Assertions.*;

class JSONBootstrapTest {

    private String commonPath;
    private String commonOutPath;
    private String correctWrapper;
    private int correctNumPrimitives;
    private int correctNumArrays;
    private int correctNumPrimitiveArrays;

    @BeforeEach
    void setUp() {
        commonPath = "src/main/resources/systematic_testing/";
        commonOutPath = "src/main/resources/out/";
    }

    @AfterEach
    void tearDown() {
    }

    //json with no boolean primitives
    //example:
    //{
    //"name": "Carlos"
    //}
    @Test
    void JSONBootstrap_object_with_non_boolean_primitives() throws IOException {
        JSONBootstrap j = new JSONBootstrap("1234", "pruebaUno", commonPath+"pruebaUno.json");
        Graph M = j.bootstrapSchema();
        j.getG_source().write(commonOutPath+"GJSONp1.ttl", Lang.TURTLE);
        M.write(commonOutPath+"GRDFSp1.ttl", Lang.TURTLE);

        Graph MGood = new Graph();
        MGood.loadModel(commonOutPath+"GRDFSp1.ttl");
        assertTrue(MGood.getModel().isIsomorphicWith(M.getModel()));
        //check if wrapper is correct
        correctWrapper = "SELECT id_,index,guid FROM pruebaUno ";
        assertEquals(correctWrapper, j.getWrapper());
        correctNumPrimitives = 3;
        assertEquals(correctNumPrimitives, j.getJSONPrimitives().size());
    }

    //json which contains boolean primitives
    //example:
    //{
    //"name": "Carlos",
    //"married": true
    //}
    @Test
    void JSONBootstrap_object_with_boolean_primitives() throws IOException {
        JSONBootstrap j = new JSONBootstrap("1234", "pruebaDos", commonPath+"pruebaDos.json");
        Graph M = j.bootstrapSchema();
        j.getG_source().write(commonOutPath+"GJSONp2.ttl", Lang.TURTLE);
        M.write(commonOutPath+"GRDFSp2.ttl", Lang.TURTLE);

        Graph MGood = new Graph();
        MGood.loadModel(commonOutPath+"GRDFSp2.ttl");
        assertTrue(MGood.getModel().isIsomorphicWith(M.getModel()));
        //check if wrapper is correct
        correctWrapper = "SELECT id_,available,index FROM pruebaDos ";
        assertEquals(correctWrapper, j.getWrapper());
        correctNumPrimitives = 3;
        assertEquals(correctNumPrimitives, j.getJSONPrimitives().size());
    }

    //example: {}
    @Test
    void JSONBootstrap_empty_JSON() throws IOException {
        JSONBootstrap j = new JSONBootstrap("1234", "pruebaCuatro", commonPath+"pruebaCuatro.json");
        Graph M = j.bootstrapSchema();
        j.getG_source().write(commonOutPath+"GJSONp4.ttl", Lang.TURTLE);
        M.write(commonOutPath+"GRDFSp4.ttl", Lang.TURTLE);

        Graph MGood = new Graph();
        MGood.loadModel(commonOutPath+"GRDFSp4.ttl");
        assertTrue(MGood.getModel().isIsomorphicWith(M.getModel()));
        correctNumPrimitives = 0;
        assertEquals(correctNumPrimitives, j.getJSONPrimitives().size());
    }

    //json that has an object which is empty
    //example:
    //{
    //"name": "Carlos",
    //"friends": {}
    //}
    @Test
    void JSONBootstrap_empty_object() throws IOException{
        JSONBootstrap j = new JSONBootstrap("1234", "pruebaCinco", commonPath+"pruebaCinco.json");
        Graph M = j.bootstrapSchema();
        j.getG_source().write(commonOutPath+"GJSONp5.ttl", Lang.TURTLE);
        M.write(commonOutPath+"GRDFSp5.ttl", Lang.TURTLE);

        Graph MGood = new Graph();
        MGood.loadModel(commonOutPath+"GRDFSp5.ttl");
        assertTrue(MGood.getModel().isIsomorphicWith(M.getModel()));
        //check if wrapper is correct
        correctWrapper = "SELECT id_,index FROM pruebaCinco ";
        assertEquals(correctWrapper, j.getWrapper());
        correctNumPrimitives = 2;
        assertEquals(correctNumPrimitives, j.getJSONPrimitives().size());
    }

    //json that has a key with spaces
    //example:
    //{
    //"name": "Carlos",
    //"place of work": "Tarragona"
    //}
    @Test
    void JSONBootstrap_key_with_spaces() throws IOException {
        JSONBootstrap j = new JSONBootstrap("1234", "pruebaSiete", commonPath+"pruebaSiete.json");
        Graph M = j.bootstrapSchema();
        j.getG_source().write(commonOutPath+"GJSONp7.ttl", Lang.TURTLE);
        M.write(commonOutPath+"GRDFSp7.ttl", Lang.TURTLE);

        Graph MGood = new Graph();
        MGood.loadModel(commonOutPath+"GRDFSp7.ttl");
        assertTrue(MGood.getModel().isIsomorphicWith(M.getModel()));
        assertEquals(1, j.getKeysWrongFormatted().size());
        //check if wrapper is  correct
        correctWrapper = "SELECT profession,id_,`place of work` AS place_of_work,book_info.title AS book_info_title,book_info.year AS book_info_year,index FROM pruebaSiete ";
        assertEquals(correctWrapper, j.getWrapper());
        correctNumPrimitives = 6;
        assertEquals(correctNumPrimitives, j.getJSONPrimitives().size());
    }

    //json that has an object which contains a key with the same keyname as one of the keys of the father
    //example:
    //{
    //"name": "Carlos",
    //"friends": {
    //  "name": "Miguel"
    //}
    //}
    @Test
    void JSONBootstrap_subobject_with_same_keyName() throws IOException {
        JSONBootstrap j = new JSONBootstrap("1234", "pruebaOcho", commonPath+"pruebaOcho.json");
        Graph M = j.bootstrapSchema();
        j.getG_source().write(commonOutPath+"GJSONp8.ttl", Lang.TURTLE);
        M.write(commonOutPath+"GRDFSp8.ttl", Lang.TURTLE);

        Graph MGood = new Graph();
        MGood.loadModel(commonOutPath+"GRDFSp8.ttl");
        assertTrue(MGood.getModel().isIsomorphicWith(M.getModel()));
        assertEquals(1, j.getKeysWrongFormatted().size());
        //check if wrapper is correct
        correctWrapper = "SELECT id_,`place of work` AS place_of_work,index,o2.`place of work` AS o2_place_of_work FROM pruebaOcho ";
        assertEquals(correctWrapper, j.getWrapper());
        correctNumPrimitives = 4;
        assertEquals(correctNumPrimitives, j.getJSONPrimitives().size());
    }

    //json that has an object which contains a key with the same keyname as one of the keys of the father, and also has an object with the same condition
    //example:
    //{
    //"name": "Carlos",
    //"friends": {
    //  "name": "Miguel",
    //  "friends: {
    //      "name": "Lucas"
    //  }
    //}
    //}
    @Test
    void JSONBootstrap_two_subobjects_with_same_keyName() throws IOException {
        JSONBootstrap j = new JSONBootstrap("1234", "pruebaNueve", commonPath+"pruebaNueve.json");
        Graph M = j.bootstrapSchema();
        j.getG_source().write(commonOutPath+"GJSONp9.ttl", Lang.TURTLE);
        M.write(commonOutPath+"GRDFSp9.ttl", Lang.TURTLE);

        Graph MGood = new Graph();
        MGood.loadModel(commonOutPath+"GRDFSp9.ttl");
        assertTrue(MGood.getModel().isIsomorphicWith(M.getModel()));
        assertEquals(1, j.getKeysWrongFormatted().size());
        //check if wrapper is correct
        correctWrapper = "SELECT o2.o3.`place of work` AS o2_o3_place_of_work,id_,`place of work` AS place_of_work,index,o2.`place of work` AS o2_place_of_work FROM pruebaNueve ";
        assertEquals(correctWrapper, j.getWrapper());
        correctNumPrimitives = 5;
        assertEquals(correctNumPrimitives, j.getJSONPrimitives().size());
    }

    //json that contains a set of different objects and subobjects
    @Test
    void JSONBootstrap_different_subobjects() throws IOException {
        JSONBootstrap j = new JSONBootstrap("1234", "pruebaDiez", commonPath+"pruebaDiez.json");
        Graph M = j.bootstrapSchema();
        j.getG_source().write(commonOutPath+"GJSONp10.ttl", Lang.TURTLE);
        M.write(commonOutPath+"GRDFSp10.ttl", Lang.TURTLE);

        Graph MGood = new Graph();
        MGood.loadModel(commonOutPath+"GRDFSp10.ttl");
        assertTrue(MGood.getModel().isIsomorphicWith(M.getModel()));
        assertEquals(5, j.getKeysWrongFormatted().size());
        //check if wrapper is correct
        correctWrapper = "SELECT friend.`friend of friend`.`friend of friend of friend`.id_ AS friend_friend_of_friend_friend_of_friend_of_friend_id_,id_,friend.`friend of friend`.index AS friend_friend_of_friend_index,friend.`friend of friend`.`friend of 1st person` AS friend_friend_of_friend_friend_of_1st_person,friend.`friend of friend`.`friend of friend of friend`.`friend of everyone` AS friend_friend_of_friend_friend_of_friend_of_friend_friend_of_everyone,friend.id_ AS friend_id_,friend.`friend of friend`.`friend of friend of friend`.index AS friend_friend_of_friend_friend_of_friend_of_friend_index,`place of work` AS place_of_work,friend.index AS friend_index,friend.`friend of friend`.id_ AS friend_friend_of_friend_id_,friend.`friend of friend`.`friend of friend of friend`.`place of work` AS friend_friend_of_friend_friend_of_friend_of_friend_place_of_work,friend.`place of work` AS friend_place_of_work,friend.`friend of friend`.`place of work` AS friend_friend_of_friend_place_of_work FROM pruebaDiez ";
        assertEquals(correctWrapper, j.getWrapper());
        correctNumPrimitives = 13;
        assertEquals(correctNumPrimitives, j.getJSONPrimitives().size());
    }

    //a json that contains a key that has dots on its keyname
    //example:
    //{
    //"name": "Carlos",
    //"place.of.work": "Tarragona"
    //}

    @Test
    void JSONBootstrap_key_with_dots() throws IOException {
        JSONBootstrap j = new JSONBootstrap("1234", "pruebaOnce", commonPath+"pruebaOnce.json");
        Graph M = j.bootstrapSchema();
        j.getG_source().write(commonOutPath+"GJSONp11.ttl", Lang.TURTLE);
        M.write(commonOutPath+"GRDFSp11.ttl", Lang.TURTLE);

        Graph MGood = new Graph();
        MGood.loadModel(commonOutPath+"GRDFSp11.ttl");
        assertTrue(MGood.getModel().isIsomorphicWith(M.getModel()));
        assertEquals(1, j.getKeysWrongFormatted().size());
        //check if wrapper is correct
        correctWrapper = "SELECT id_,`place.of.work` AS place_of_work,index FROM pruebaOnce ";
        assertEquals(correctWrapper, j.getWrapper());
        correctNumPrimitives = 3;
        assertEquals(correctNumPrimitives, j.getJSONPrimitives().size());
    }

    //a json that contains an array of Strings
    //example:
    //{
    //"arraystring": [ "Test" ]
    //}

    @Test
    void JSONBootstrap_array_string() throws IOException {
        JSONBootstrap j = new JSONBootstrap("1234", "arrayUno", commonPath+"arrayUno.json");
        Graph M = j.bootstrapSchema();
        j.getG_source().write(commonOutPath+"GJSONa1.ttl", Lang.TURTLE);
        M.write(commonOutPath+"GRDFSa1.ttl", Lang.TURTLE);

        Graph MGood = new Graph();
        MGood.loadModel(commonOutPath+"GRDFSa1.ttl");
        assertTrue(MGood.getModel().isIsomorphicWith(M.getModel()));
        //check if wrapper is correct
        correctNumArrays = 1;
        correctNumPrimitiveArrays = 1;
        assertEquals(correctNumPrimitiveArrays, j.getPrimitiveArrays().size());
        assertEquals(correctNumArrays, j.getFathers().size());
    }

    //a json that contains an array of Numbers
    //example:
    //{
    //"arrayNumber": [ 1, 2, 3 ]
    //}

    @Test
    void JSONBootstrap_array_number() throws IOException {
        JSONBootstrap j = new JSONBootstrap("1234", "arrayDos", commonPath+"arrayDos.json");
        Graph M = j.bootstrapSchema();
        j.getG_source().write(commonOutPath+"GJSONa2.ttl", Lang.TURTLE);
        M.write(commonOutPath+"GRDFSa2.ttl", Lang.TURTLE);

        Graph MGood = new Graph();
        MGood.loadModel(commonOutPath+"GRDFSa2.ttl");
        assertTrue(MGood.getModel().isIsomorphicWith(M.getModel()));
        //check if wrapper is correct
        correctNumArrays = 1;
        correctNumPrimitiveArrays = 1;
        assertEquals(correctNumPrimitiveArrays, j.getPrimitiveArrays().size());
        assertEquals(correctNumArrays, j.getFathers().size());
    }

    //a json that contains an array of Booleans
    //example:
    //{
    //"arraystring": [ true, false, false ]
    //}

    @Test
    void JSONBootstrap_array_boolean() throws IOException {
        JSONBootstrap j = new JSONBootstrap("1234", "arrayTres", commonPath+"arrayTres.json");
        Graph M = j.bootstrapSchema();
        j.getG_source().write(commonOutPath+"GJSONa3.ttl", Lang.TURTLE);
        M.write(commonOutPath+"GRDFSa3.ttl", Lang.TURTLE);

        Graph MGood = new Graph();
        MGood.loadModel(commonOutPath+"GRDFSa3.ttl");
        assertTrue(MGood.getModel().isIsomorphicWith(M.getModel()));
        //check if wrapper is correct
        correctNumArrays = 1;
        correctNumPrimitiveArrays = 1;
        assertEquals(correctNumPrimitiveArrays, j.getPrimitiveArrays().size());
        assertEquals(correctNumArrays, j.getFathers().size());
    }

    //a json that contains an array of Objects
    //example:
    //{
    //"arraystring": [ {
    //  "subkey1": 1,
    //  "subkey2": 2
    // } ]
    //}

    @Test
    void JSONBootstrap_array_object() throws IOException {
        JSONBootstrap j = new JSONBootstrap("1234", "arrayCuatro", commonPath+"arrayCuatro.json");
        Graph M = j.bootstrapSchema();
        j.getG_source().write(commonOutPath+"GJSONa4.ttl", Lang.TURTLE);
        M.write(commonOutPath+"GRDFSa4.ttl", Lang.TURTLE);

        Graph MGood = new Graph();
        MGood.loadModel(commonOutPath+"GRDFSa4.ttl");
        assertTrue(MGood.getModel().isIsomorphicWith(M.getModel()));
        //check if wrapper is correct
        correctNumArrays = 1;
        correctNumPrimitiveArrays = 0;
        correctNumPrimitives = 2;
        assertEquals(correctNumPrimitiveArrays, j.getPrimitiveArrays().size());
        assertEquals(correctNumArrays, j.getFathers().size());
        assertEquals(correctNumPrimitives, j.getJSONPrimitives().size());
    }

    //a json that contains a multiarray of strings
    //example:
    //{
    //"multiarrayString": [ ["hello"], ["world"] ]
    //}

    @Test
    void JSONBootstrap_multiarray_string() throws IOException {
        JSONBootstrap j = new JSONBootstrap("1234", "arrayCinco", commonPath+"arrayCinco.json");
        Graph M = j.bootstrapSchema();
        j.getG_source().write(commonOutPath+"GJSONa5.ttl", Lang.TURTLE);
        M.write(commonOutPath+"GRDFSa5.ttl", Lang.TURTLE);

        Graph MGood = new Graph();
        MGood.loadModel(commonOutPath+"GRDFSa5.ttl");
        assertTrue(MGood.getModel().isIsomorphicWith(M.getModel()));
        //check if wrapper is correct
        correctNumArrays = 1;
        correctNumPrimitiveArrays = 1;
        correctNumPrimitives = 0;
        assertEquals(correctNumPrimitiveArrays, j.getPrimitiveArrays().size());
        assertEquals(correctNumArrays, j.getFathers().size());
        assertEquals(correctNumPrimitives, j.getJSONPrimitives().size());
    }

    //a json that contains a multiarray of numbers
    //example:
    //{
    //"multiarrayNumbers": [ [1], [2] ]
    //}

    @Test
    void JSONBootstrap_multiarray_number() throws IOException {
        JSONBootstrap j = new JSONBootstrap("1234", "arraySeis", commonPath+"arraySeis.json");
        Graph M = j.bootstrapSchema();
        j.getG_source().write(commonOutPath+"GJSONa6.ttl", Lang.TURTLE);
        M.write(commonOutPath+"GRDFSa6.ttl", Lang.TURTLE);

        Graph MGood = new Graph();
        MGood.loadModel(commonOutPath+"GRDFSa6.ttl");
        assertTrue(MGood.getModel().isIsomorphicWith(M.getModel()));
        //check if wrapper is correct
        correctNumArrays = 1;
        correctNumPrimitiveArrays = 1;
        correctNumPrimitives = 0;
        assertEquals(correctNumPrimitiveArrays, j.getPrimitiveArrays().size());
        assertEquals(correctNumArrays, j.getFathers().size());
        assertEquals(correctNumPrimitives, j.getJSONPrimitives().size());
    }

    //a json that contains a multiarray of booleans
    //example:
    //{
    //"multiarrayBooleans": [ [true], [false] ]
    //}

    @Test
    void JSONBootstrap_multiarray_boolean() throws IOException {
        JSONBootstrap j = new JSONBootstrap("1234", "arraySiete", commonPath+"arraySiete.json");
        Graph M = j.bootstrapSchema();
        j.getG_source().write(commonOutPath+"GJSONa7.ttl", Lang.TURTLE);
        M.write(commonOutPath+"GRDFSa7.ttl", Lang.TURTLE);

        Graph MGood = new Graph();
        MGood.loadModel(commonOutPath+"GRDFSa7.ttl");
        assertTrue(MGood.getModel().isIsomorphicWith(M.getModel()));
        //check if wrapper is correct
        correctNumArrays = 1;
        correctNumPrimitiveArrays = 1;
        correctNumPrimitives = 0;
        assertEquals(correctNumPrimitiveArrays, j.getPrimitiveArrays().size());
        assertEquals(correctNumArrays, j.getFathers().size());
        assertEquals(correctNumPrimitives, j.getJSONPrimitives().size());
    }

    //a json that contains a multiarray of objects
    //example:
    //{
    //"multiarrayObjects": [ [ { "name": "Alberto", "Profession": "Doctor" }, { "name": "Jose", "Profession": "Teacher" } ] ]
    //}

    @Test
    void JSONBootstrap_multiarray_objects() throws IOException {
        JSONBootstrap j = new JSONBootstrap("1234", "arrayOcho", commonPath+"arrayOcho.json");
        Graph M = j.bootstrapSchema();
        j.getG_source().write(commonOutPath+"GJSONa8.ttl", Lang.TURTLE);
        M.write(commonOutPath+"GRDFSa8.ttl", Lang.TURTLE);

        Graph MGood = new Graph();
        MGood.loadModel(commonOutPath+"GRDFSa8.ttl");
        assertTrue(MGood.getModel().isIsomorphicWith(M.getModel()));
        //check if wrapper is correct
        correctNumArrays = 1;
        correctNumPrimitiveArrays = 0;
        correctNumPrimitives = 2;
        assertEquals(correctNumPrimitiveArrays, j.getPrimitiveArrays().size());
        assertEquals(correctNumArrays, j.getFathers().size());
        assertEquals(correctNumPrimitives, j.getJSONPrimitives().size());
    }

    //a json that is complex, i.e. has a lot of pairs of key/value and objects/subobjects
    @Test
    void JSONBootstrap_complex_JSON_1() throws IOException {
        JSONBootstrap j = new JSONBootstrap("1234", "complexJSON1", commonPath+"complexJSON1.json");
        Graph M = j.bootstrapSchema();
        j.getG_source().write(commonOutPath+"GJSONcj1.ttl", Lang.TURTLE);
        M.write(commonOutPath+"GRDFScj1.ttl", Lang.TURTLE);

        Graph MGood = new Graph();
        MGood.loadModel(commonOutPath+"GRDFScj1.ttl");
        assertTrue(MGood.getModel().isIsomorphicWith(M.getModel()));
        //check if wrapper is correct
        correctWrapper = "SELECT locations.location1.address AS locations_location1_address,url_force,locations.location1.name AS locations_location1_name,links.link2.title AS links_link2_title,centre.longitude AS centre_longitude,description,links.link1.title AS links_link1_title,contact_details.facebook AS contact_details_facebook,locations.location1.postcode AS locations_location1_postcode,locations.location1.type AS locations_location1_type,population,links.link1.url AS links_link1_url,contact_details.telephone AS contact_details_telephone,centre.latitude AS centre_latitude,contact_details.email AS contact_details_email,locations.location1.description AS locations_location1_description,name,id,links.link2.url AS links_link2_url,contact_details.twitter AS contact_details_twitter FROM complexJSON1 ";
        assertEquals(correctWrapper, j.getWrapper());
        correctNumPrimitives = 20;
        assertEquals(correctNumPrimitives, j.getJSONPrimitives().size());
    }

    //a json that is complex, i.e. has a lot of pairs of key/value and objects/subobjects
    @Test
    void JSONBootstrap_complex_JSON_2() throws IOException {
        JSONBootstrap j = new JSONBootstrap("1234", "complexJSON2", commonPath+"complexJSON2.json");
        Graph M = j.bootstrapSchema();
        j.getG_source().write(commonOutPath+"GJSONcj2.ttl", Lang.TURTLE);
        M.write(commonOutPath+"GRDFScj2.ttl", Lang.TURTLE);

        Graph MGood = new Graph();
        MGood.loadModel(commonOutPath+"GRDFScj2.ttl");
        assertTrue(MGood.getModel().isIsomorphicWith(M.getModel()));
        //check if wrapper is correct
        correctWrapper = "SELECT date,death.place.placeType AS death_place_placeType,gender,`active Places`.activePlace2.placeName AS active_Places_activePlace2_placeName,birth.place.placeName AS birth_place_placeName,activePlaceCount,death.place.name AS death_place_name,movements.movement1.id AS movements_movement1_id,`active Places`.activePlace2.placeType AS active_Places_activePlace2_placeType,movements.movement1.era.id AS movements_movement1_era_id,movements.movement2.era.id AS movements_movement2_era_id,id,totalWorks,death.place.is_death AS death_place_is_death,birth.place.name AS birth_place_name,movements.movement1.name AS movements_movement1_name,startLetter,death.time.startYear AS death_time_startYear,`active Places`.activePlace1.placeName AS active_Places_activePlace1_placeName,birth.time.startYear AS birth_time_startYear,birth.place.placeType AS birth_place_placeType,mda,movements.movement2.era.name AS movements_movement2_era_name,movements.movement2.id AS movements_movement2_id,url,`active Places`.activePlace1.name AS active_Places_activePlace1_name,movements.movement1.era.name AS movements_movement1_era_name,birthYear,`active Places`.activePlace1.placeType AS active_Places_activePlace1_placeType,death.place.placeName AS death_place_placeName,`active Places`.activePlace2.name AS active_Places_activePlace2_name,movements.movement2.name AS movements_movement2_name,fc FROM complexJSON2 ";
        assertEquals(correctWrapper, j.getWrapper());
        correctNumPrimitives = 33;
        assertEquals(correctNumPrimitives, j.getJSONPrimitives().size());
    }

    //a json that is complex, i.e. has a lot of pairs of key/value and objects/subobjects and arrays/subarrays

    @Test
    void JSONBootstrap_complex_JSON_3() throws IOException {
        JSONBootstrap j = new JSONBootstrap("1234", "complexJSON3", commonPath+"complexJSON3.json");
        Graph M = j.bootstrapSchema();
        j.getG_source().write(commonOutPath+"GJSONcj3.ttl", Lang.TURTLE);
        M.write(commonOutPath+"GRDFScj3.ttl", Lang.TURTLE);

        Graph MGood = new Graph();
        MGood.loadModel(commonOutPath+"GRDFScj3.ttl");
        assertTrue(MGood.getModel().isIsomorphicWith(M.getModel()));
        //check if wrapper is correct
        correctNumArrays = 3;
        correctNumPrimitiveArrays = 1;
        correctNumPrimitives = 5;
        assertEquals(correctNumPrimitiveArrays, j.getPrimitiveArrays().size());
        assertEquals(correctNumArrays, j.getFathers().size());
        assertEquals(correctNumPrimitives, j.getJSONPrimitives().size());
    }

    //a json that is complex, i.e. has a lot of pairs of key/value and objects/subobjects and arrays/subarrays

    @Test
    void JSONBootstrap_complex_JSON_4() throws IOException {
        JSONBootstrap j = new JSONBootstrap("1234", "complexJSON4", commonPath+"complexJSON4.json");
        Graph M = j.bootstrapSchema();
        j.getG_source().write(commonOutPath+"GJSONcj4.ttl", Lang.TURTLE);
        M.write(commonOutPath+"GRDFScj4.ttl", Lang.TURTLE);

        Graph MGood = new Graph();
        MGood.loadModel(commonOutPath+"GRDFScj4.ttl");
        assertTrue(MGood.getModel().isIsomorphicWith(M.getModel()));
        //check if wrapper is correct
        correctNumArrays = 1;
        correctNumPrimitiveArrays = 0;
        correctNumPrimitives = 4;
        assertEquals(correctNumPrimitiveArrays, j.getPrimitiveArrays().size());
        assertEquals(correctNumArrays, j.getFathers().size());
        assertEquals(correctNumPrimitives, j.getJSONPrimitives().size());
    }

    //a json that is complex, i.e. has a lot of pairs of key/value and objects/subobjects and arrays/subarrays

    @Test
    void JSONBootstrap_complex_JSON_5() throws IOException {
        JSONBootstrap j = new JSONBootstrap("1234", "complexJSON5", commonPath+"complexJSON5.json");
        Graph M = j.bootstrapSchema();
        j.getG_source().write(commonOutPath+"GJSONcj5.ttl", Lang.TURTLE);
        M.write(commonOutPath+"GRDFScj5.ttl", Lang.TURTLE);

        Graph MGood = new Graph();
        MGood.loadModel(commonOutPath+"GRDFScj5.ttl");
        assertTrue(MGood.getModel().isIsomorphicWith(M.getModel()));
        //check if wrapper is correct
        correctNumArrays = 2;
        correctNumPrimitiveArrays = 1;
        correctNumPrimitives = 7;
        assertEquals(correctNumPrimitiveArrays, j.getPrimitiveArrays().size());
        assertEquals(correctNumArrays, j.getFathers().size());
        assertEquals(correctNumPrimitives, j.getJSONPrimitives().size());
    }

    //a json that is complex, i.e. has a lot of pairs of key/value and objects/subobjects and arrays/subarrays

    @Test
    void JSONBootstrap_complex_JSON_6() throws IOException {
        JSONBootstrap j = new JSONBootstrap("1234", "complexJSON6", commonPath+"complexJSON6.json");
        Graph M = j.bootstrapSchema();
        j.getG_source().write(commonOutPath+"GJSONcj6.ttl", Lang.TURTLE);
        M.write(commonOutPath+"GRDFScj6.ttl", Lang.TURTLE);

        Graph MGood = new Graph();
        MGood.loadModel(commonOutPath+"GRDFScj6.ttl");
        assertTrue(MGood.getModel().isIsomorphicWith(M.getModel()));
        //check if wrapper is correct
        correctNumArrays = 4;
        correctNumPrimitiveArrays = 0;
        correctNumPrimitives = 43;
        assertEquals(correctNumPrimitiveArrays, j.getPrimitiveArrays().size());
        assertEquals(correctNumArrays, j.getFathers().size());
        assertEquals(correctNumPrimitives, j.getJSONPrimitives().size());
    }

    //a json that is complex, i.e. has a lot of pairs of key/value and objects/subobjects and arrays/subarrays

    @Test
    void JSONBootstrap_complex_JSON_7() throws IOException {
        JSONBootstrap j = new JSONBootstrap("1234", "complexJSON7", commonPath+"complexJSON7.json");
        Graph M = j.bootstrapSchema();
        j.getG_source().write(commonOutPath+"GJSONcj7.ttl", Lang.TURTLE);
        M.write(commonOutPath+"GRDFScj7.ttl", Lang.TURTLE);

        Graph MGood = new Graph();
        MGood.loadModel(commonOutPath+"GRDFScj7.ttl");
        assertTrue(MGood.getModel().isIsomorphicWith(M.getModel()));
        //check if wrapper is correct
        correctNumArrays = 2;
        correctNumPrimitiveArrays = 0;
        correctNumPrimitives = 18;
        assertEquals(correctNumPrimitiveArrays, j.getPrimitiveArrays().size());
        assertEquals(correctNumArrays, j.getFathers().size());
        assertEquals(correctNumPrimitives, j.getJSONPrimitives().size());
    }

    //a json that is complex, i.e. has a lot of pairs of key/value and objects/subobjects and arrays/subarrays

    @Test
    void JSONBootstrap_complex_JSON_8() throws IOException {
        JSONBootstrap j = new JSONBootstrap("1234", "complexJSON8", commonPath+"complexJSON8.json");
        Graph M = j.bootstrapSchema();
        j.getG_source().write(commonOutPath+"GJSONcj8.ttl", Lang.TURTLE);
        M.write(commonOutPath+"GRDFScj8.ttl", Lang.TURTLE);

        Graph MGood = new Graph();
        MGood.loadModel(commonOutPath+"GRDFScj8.ttl");
        assertTrue(MGood.getModel().isIsomorphicWith(M.getModel()));
        //check if wrapper is correct
        correctNumArrays = 2;
        correctNumPrimitiveArrays = 0;
        correctNumPrimitives = 25;
        assertEquals(correctNumPrimitiveArrays, j.getPrimitiveArrays().size());
        assertEquals(correctNumArrays, j.getFathers().size());
        assertEquals(correctNumPrimitives, j.getJSONPrimitives().size());
    }

    //a json that is complex, i.e. has a lot of pairs of key/value and objects/subobjects and arrays/subarrays

    @Test
    void JSONBootstrap_complex_JSON_9() throws IOException {
        JSONBootstrap j = new JSONBootstrap("1234", "complexJSON9", commonPath+"complexJSON9.json");
        Graph M = j.bootstrapSchema();
        j.getG_source().write(commonOutPath+"GJSONcj9.ttl", Lang.TURTLE);
        M.write(commonOutPath+"GRDFScj9.ttl", Lang.TURTLE);

        Graph MGood = new Graph();
        MGood.loadModel(commonOutPath+"GRDFScj9.ttl");
        assertTrue(MGood.getModel().isIsomorphicWith(M.getModel()));
        //check if wrapper is correct
        correctNumArrays = 2;
        correctNumPrimitiveArrays = 0;
        correctNumPrimitives = 34;
        assertEquals(correctNumPrimitiveArrays, j.getPrimitiveArrays().size());
        assertEquals(correctNumArrays, j.getFathers().size());
        assertEquals(correctNumPrimitives, j.getJSONPrimitives().size());
    }

    //a json that is complex, i.e. has a lot of pairs of key/value and objects/subobjects and arrays/subarrays

    @Test
    void JSONBootstrap_complex_JSON_10() throws IOException {
        JSONBootstrap j = new JSONBootstrap("1234", "complexJSON10", commonPath+"complexJSON10.json");
        Graph M = j.bootstrapSchema();
        j.getG_source().write(commonOutPath+"GJSONcj10.ttl", Lang.TURTLE);
        M.write(commonOutPath+"GRDFScj10.ttl", Lang.TURTLE);

        Graph MGood = new Graph();
        MGood.loadModel(commonOutPath+"GRDFScj10.ttl");
        assertTrue(MGood.getModel().isIsomorphicWith(M.getModel()));
        //check if wrapper is correct
        correctNumArrays = 2;
        correctNumPrimitiveArrays = 0;
        correctNumPrimitives = 9;
        assertEquals(correctNumPrimitiveArrays, j.getPrimitiveArrays().size());
        assertEquals(correctNumArrays, j.getFathers().size());
        assertEquals(correctNumPrimitives, j.getJSONPrimitives().size());
    }
}