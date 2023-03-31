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
    private String nameDataset;
    private String commonOutPath;
    private String correctWrapper;

    @BeforeEach
    void setUp() {
        commonPath = "src/main/resources/systematic_testing/";
        commonOutPath = "src/main/resources/out/";
        nameDataset = "dataset1";
    }

    @AfterEach
    void tearDown() {
    }

    //the following tests are done with JSONs that DO NOT have Arrays

    @Test
    void JSONBootstrap_object_with_non_boolean_primitives() throws IOException {
        JSONBootstrap j = new JSONBootstrap("1234", nameDataset, commonPath+"pruebaUno.json");
        Graph M = j.bootstrapSchema();
        j.getG_source().write(commonOutPath+"GJSONp1.ttl", Lang.TURTLE);
        M.write(commonOutPath+"GRDFSp1.ttl", Lang.TURTLE);

        Graph MGood = new Graph();
        MGood.loadModel(commonOutPath+"GRDFSp1.ttl");
        assertTrue(MGood.getModel().isIsomorphicWith(M.getModel()));
        //check if wrapper is correct
        correctWrapper = "SELECT id_,index,guid FROM dataset1 ";
        assertEquals(correctWrapper, j.getWrapper());
    }

    @Test
    void JSONBootstrap_object_with_boolean_primitives() throws IOException {
        JSONBootstrap j = new JSONBootstrap("1234", nameDataset, commonPath+"pruebaDos.json");
        Graph M = j.bootstrapSchema();
        j.getG_source().write(commonOutPath+"GJSONp2.ttl", Lang.TURTLE);
        M.write(commonOutPath+"GRDFSp2.ttl", Lang.TURTLE);

        Graph MGood = new Graph();
        MGood.loadModel(commonOutPath+"GRDFSp2.ttl");
        assertTrue(MGood.getModel().isIsomorphicWith(M.getModel()));
        //check if wrapper is correct
        correctWrapper = "SELECT id_,available,index FROM dataset1 ";
        assertEquals(correctWrapper, j.getWrapper());
    }

    @Test
    void JSONBootstrap_empty_JSON() throws IOException {
        JSONBootstrap j = new JSONBootstrap("1234", nameDataset, commonPath+"pruebaCuatro.json");
        Graph M = j.bootstrapSchema();
        j.getG_source().write(commonOutPath+"GJSONp4.ttl", Lang.TURTLE);
        M.write(commonOutPath+"GRDFSp4.ttl", Lang.TURTLE);

        Graph MGood = new Graph();
        MGood.loadModel(commonOutPath+"GRDFSp4.ttl");
        assertTrue(MGood.getModel().isIsomorphicWith(M.getModel()));
    }

    @Test
    void JSONBootstrap_empty_object() throws IOException{
        JSONBootstrap j = new JSONBootstrap("1234", nameDataset, commonPath+"pruebaCinco.json");
        Graph M = j.bootstrapSchema();
        j.getG_source().write(commonOutPath+"GJSONp5.ttl", Lang.TURTLE);
        M.write(commonOutPath+"GRDFSp5.ttl", Lang.TURTLE);

        Graph MGood = new Graph();
        MGood.loadModel(commonOutPath+"GRDFSp5.ttl");
        assertTrue(MGood.getModel().isIsomorphicWith(M.getModel()));
        //check if wrapper is correct
        correctWrapper = "SELECT id_,index FROM dataset1 ";
        assertEquals(correctWrapper, j.getWrapper());
    }

    @Test
    void JSONBootstrap_key_with_spaces() throws IOException {
        JSONBootstrap j = new JSONBootstrap("1234", nameDataset, commonPath+"pruebaSiete.json");
        Graph M = j.bootstrapSchema();
        j.getG_source().write(commonOutPath+"GJSONp7.ttl", Lang.TURTLE);
        M.write(commonOutPath+"GRDFSp7.ttl", Lang.TURTLE);

        Graph MGood = new Graph();
        MGood.loadModel(commonOutPath+"GRDFSp7.ttl");
        assertTrue(MGood.getModel().isIsomorphicWith(M.getModel()));
        assertEquals(1, j.getKeysWrongFormatted().size());
        //check if wrapper is  correct
        correctWrapper = "SELECT id_,`place of work` AS place_of_work,index FROM dataset1 ";
        assertEquals(correctWrapper, j.getWrapper());
    }

    @Test
    void JSONBootstrap_subobject_with_same_keyName() throws IOException {
        JSONBootstrap j = new JSONBootstrap("1234", nameDataset, commonPath+"pruebaOcho.json");
        Graph M = j.bootstrapSchema();
        j.getG_source().write(commonOutPath+"GJSONp8.ttl", Lang.TURTLE);
        M.write(commonOutPath+"GRDFSp8.ttl", Lang.TURTLE);

        Graph MGood = new Graph();
        MGood.loadModel(commonOutPath+"GRDFSp8.ttl");
        assertTrue(MGood.getModel().isIsomorphicWith(M.getModel()));
        assertEquals(1, j.getKeysWrongFormatted().size());
        //check if wrapper is correct
        correctWrapper = "SELECT id_,`place of work` AS place_of_work,index,o2.`place of work` AS o2_place_of_work FROM dataset1 ";
        assertEquals(correctWrapper, j.getWrapper());
    }

    @Test
    void JSONBootstrap_two_subobjects_with_same_keyName() throws IOException {
        JSONBootstrap j = new JSONBootstrap("1234", nameDataset, commonPath+"pruebaNueve.json");
        Graph M = j.bootstrapSchema();
        j.getG_source().write(commonOutPath+"GJSONp9.ttl", Lang.TURTLE);
        M.write(commonOutPath+"GRDFSp9.ttl", Lang.TURTLE);

        Graph MGood = new Graph();
        MGood.loadModel(commonOutPath+"GRDFSp9.ttl");
        assertTrue(MGood.getModel().isIsomorphicWith(M.getModel()));
        assertEquals(1, j.getKeysWrongFormatted().size());
        //check if wrapper is correct
        correctWrapper = "SELECT o2.o3.`place of work` AS o2_o3_place_of_work,id_,`place of work` AS place_of_work,index,o2.`place of work` AS o2_place_of_work FROM dataset1 ";
        assertEquals(correctWrapper, j.getWrapper());
    }

    @Test
    void JSONBootstrap_different_subobjects() throws IOException {
        JSONBootstrap j = new JSONBootstrap("1234", nameDataset, commonPath+"pruebaDiez.json");
        Graph M = j.bootstrapSchema();
        j.getG_source().write(commonOutPath+"GJSONp10.ttl", Lang.TURTLE);
        M.write(commonOutPath+"GRDFSp10.ttl", Lang.TURTLE);

        Graph MGood = new Graph();
        MGood.loadModel(commonOutPath+"GRDFSp10.ttl");
        assertTrue(MGood.getModel().isIsomorphicWith(M.getModel()));
        assertEquals(5, j.getKeysWrongFormatted().size());
        //check if wrapper is correct
        correctWrapper = "SELECT friend.`friend of friend`.`friend of friend of friend`.id_ AS friend_friend_of_friend_friend_of_friend_of_friend_id_,id_,friend.`friend of friend`.index AS friend_friend_of_friend_index,friend.`friend of friend`.`friend of 1st person` AS friend_friend_of_friend_friend_of_1st_person,friend.`friend of friend`.`friend of friend of friend`.`friend of everyone` AS friend_friend_of_friend_friend_of_friend_of_friend_friend_of_everyone,friend.id_ AS friend_id_,friend.`friend of friend`.`friend of friend of friend`.index AS friend_friend_of_friend_friend_of_friend_of_friend_index,`place of work` AS place_of_work,friend.index AS friend_index,friend.`friend of friend`.id_ AS friend_friend_of_friend_id_,friend.`friend of friend`.`friend of friend of friend`.`place of work` AS friend_friend_of_friend_friend_of_friend_of_friend_place_of_work,friend.`place of work` AS friend_place_of_work,friend.`friend of friend`.`place of work` AS friend_friend_of_friend_place_of_work FROM dataset1 ";
        assertEquals(correctWrapper, j.getWrapper());
    }

    @Test
    void JSONBootstrap_key_with_dots() throws IOException {
        JSONBootstrap j = new JSONBootstrap("1234", nameDataset, commonPath+"pruebaOnce.json");
        Graph M = j.bootstrapSchema();
        j.getG_source().write(commonOutPath+"GJSONp11.ttl", Lang.TURTLE);
        M.write(commonOutPath+"GRDFSp11.ttl", Lang.TURTLE);

        Graph MGood = new Graph();
        MGood.loadModel(commonOutPath+"GRDFSp11.ttl");
        assertTrue(MGood.getModel().isIsomorphicWith(M.getModel()));
        assertEquals(1, j.getKeysWrongFormatted().size());
        //check if wrapper is correct
        correctWrapper = "SELECT id_,`place.of.work` AS place_of_work,index FROM dataset1 ";
        assertEquals(correctWrapper, j.getWrapper());
    }

    @Test
    void JSONBootstrap_long_JSON_1() throws IOException {
        JSONBootstrap j = new JSONBootstrap("1234", nameDataset, commonPath+"pruebaGrandeUno.json");
        Graph M = j.bootstrapSchema();
        j.getG_source().write(commonOutPath+"GJSONpg1.ttl", Lang.TURTLE);
        M.write(commonOutPath+"GRDFSpg1.ttl", Lang.TURTLE);

        Graph MGood = new Graph();
        MGood.loadModel(commonOutPath+"GRDFSpg1.ttl");
        assertTrue(MGood.getModel().isIsomorphicWith(M.getModel()));
        //check if wrapper is correct
        correctWrapper = "SELECT locations.location1.address AS locations_location1_address,url_force,locations.location1.name AS locations_location1_name,links.link2.title AS links_link2_title,centre.longitude AS centre_longitude,description,links.link1.title AS links_link1_title,contact_details.facebook AS contact_details_facebook,locations.location1.postcode AS locations_location1_postcode,locations.location1.type AS locations_location1_type,population,links.link1.url AS links_link1_url,contact_details.telephone AS contact_details_telephone,centre.latitude AS centre_latitude,contact_details.email AS contact_details_email,locations.location1.description AS locations_location1_description,name,id,links.link2.url AS links_link2_url,contact_details.twitter AS contact_details_twitter FROM dataset1 ";
        assertEquals(correctWrapper, j.getWrapper());
    }

    @Test
    void JSONBootstrap_long_JSON_2() throws IOException {
        JSONBootstrap j = new JSONBootstrap("1234", nameDataset, commonPath+"pruebaGrandeDos.json");
        Graph M = j.bootstrapSchema();
        j.getG_source().write(commonOutPath+"GJSONpg2.ttl", Lang.TURTLE);
        M.write(commonOutPath+"GRDFSpg2.ttl", Lang.TURTLE);

        Graph MGood = new Graph();
        MGood.loadModel(commonOutPath+"GRDFSpg2.ttl");
        assertTrue(MGood.getModel().isIsomorphicWith(M.getModel()));
        //check if wrapper is correct
        correctWrapper = "SELECT date,death.place.placeType AS death_place_placeType,gender,birth.place.placeName AS birth_place_placeName,activePlaceCount,death.place.name AS death_place_name,movements.movement1.id AS movements_movement1_id,movements.movement1.era.id AS movements_movement1_era_id,movements.movement2.era.id AS movements_movement2_era_id,activePlaces.activePlace1.placeName AS activePlaces_activePlace1_placeName,id,totalWorks,birth.place.name AS birth_place_name,movements.movement1.name AS movements_movement1_name,startLetter,death.time.startYear AS death_time_startYear,birth.time.startYear AS birth_time_startYear,birth.place.placeType AS birth_place_placeType,mda,movements.movement2.era.name AS movements_movement2_era_name,movements.movement2.id AS movements_movement2_id,url,activePlaces.activePlace1.name AS activePlaces_activePlace1_name,activePlaces.activePlace2.placeType AS activePlaces_activePlace2_placeType,movements.movement1.era.name AS movements_movement1_era_name,activePlaces.activePlace1.placeType AS activePlaces_activePlace1_placeType,birthYear,death.place.placeName AS death_place_placeName,movements.movement2.name AS movements_movement2_name,activePlaces.activePlace2.name AS activePlaces_activePlace2_name,activePlaces.activePlace2.placeName AS activePlaces_activePlace2_placeName,fc FROM dataset1 ";
        assertEquals(correctWrapper, j.getWrapper());
    }

    @Test
    void JSONBootstrap_long_JSON_3() throws IOException {
        JSONBootstrap j = new JSONBootstrap("1234", nameDataset, commonPath+"pruebaGrandeTres.json");
        Graph M = j.bootstrapSchema();
        j.getG_source().write(commonOutPath+"GJSONpg3.ttl", Lang.TURTLE);
        M.write(commonOutPath+"GRDFSpg3.ttl", Lang.TURTLE);

        Graph MGood = new Graph();
        MGood.loadModel(commonOutPath+"GRDFSpg3.ttl");
        assertTrue(MGood.getModel().isIsomorphicWith(M.getModel()));
        assertEquals(2, j.getKeysWrongFormatted().size());
        //check if wrapper is correct
        correctWrapper = "SELECT dateRange.endYear AS dateRange_endYear,contributors.`contributor 1`.date AS contributors_contributor_1_date,all_artists,contributors.`contributor 1`.role AS contributors_contributor_1_role,movements.`movement 1`.era.name AS movements_movement_1_era_name,groupTitle,acquisitionYear,contributors.`contributor 1`.id AS contributors_contributor_1_id,medium,units,title,acno,movementCount,creditLine,contributors.`contributor 1`.mda AS contributors_contributor_1_mda,id,dateRange.text AS dateRange_text,height,thumbnailUrl,dateText,movements.`movement 1`.name AS movements_movement_1_name,contributors.`contributor 1`.displayOrder AS contributors_contributor_1_displayOrder,contributors.`contributor 1`.birthYear AS contributors_contributor_1_birthYear,contributors.`contributor 1`.fc AS contributors_contributor_1_fc,contributors.`contributor 1`.gender AS contributors_contributor_1_gender,dateRange.startYear AS dateRange_startYear,classification,url,contributors.`contributor 1`.startLetter AS contributors_contributor_1_startLetter,movements.`movement 1`.era.id AS movements_movement_1_era_id,width,contributorCount,movements.`movement 1`.id AS movements_movement_1_id,dimensions FROM dataset1 ";
        assertEquals(correctWrapper, j.getWrapper());
    }

    @Test
    void JSONBootstrap_long_JSON_4() throws IOException {
        JSONBootstrap j = new JSONBootstrap("1234", nameDataset, commonPath+"pruebaGrandeCuatro.json");
        Graph M = j.bootstrapSchema();
        j.getG_source().write(commonOutPath+"GJSONpg4.ttl", Lang.TURTLE);
        M.write(commonOutPath+"GRDFSpg4.ttl", Lang.TURTLE);

        Graph MGood = new Graph();
        MGood.loadModel(commonOutPath+"GRDFSpg4.ttl");
        assertTrue(MGood.getModel().isIsomorphicWith(M.getModel()));
        assertEquals(1, j.getKeysWrongFormatted().size());
        //check if wrapper is correct
        correctWrapper = "SELECT dateRange.endYear AS dateRange_endYear,contributors.`contributor 1`.date AS contributors_contributor_1_date,all_artists,contributors.`contributor 1`.role AS contributors_contributor_1_role,groupTitle,acquisitionYear,subjects.id AS subjects_id,contributors.`contributor 1`.id AS contributors_contributor_1_id,medium,units,thumbnailCopyright,title,foreignTitle,acno,movementCount,creditLine,subjects.name AS subjects_name,inscription,contributors.`contributor 1`.mda AS contributors_contributor_1_mda,id,dateRange.text AS dateRange_text,height,thumbnailUrl,dateText,contributors.`contributor 1`.displayOrder AS contributors_contributor_1_displayOrder,contributors.`contributor 1`.birthYear AS contributors_contributor_1_birthYear,contributors.`contributor 1`.fc AS contributors_contributor_1_fc,contributors.`contributor 1`.gender AS contributors_contributor_1_gender,dateRange.startYear AS dateRange_startYear,classification,url,contributors.`contributor 1`.startLetter AS contributors_contributor_1_startLetter,depth,width,contributorCount,dimensions,subjectCount FROM dataset1 ";
        assertEquals(correctWrapper, j.getWrapper());
    }
}