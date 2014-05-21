<?php

/**
 * Performs commands for the search engine.
 * @author : Ramzi Sh. Alqrainy - ramzi.alqrainy@gmail.com
 * @copyright Copyright (c) 2013
 * @version 0.1
 */
class SearchCommand extends CConsoleCommand {

    private static $lock = null;
    private static $lock_fn = null;
    private static $lock_fl = null;
    private static $dataSolr = 0;
    private static $all_countries = array();
    private static $all_cities = array();
    private static $countries_ids = array(1 => 1, 2 => 1, 4 => 1, 5 => 1, 6 => 1, 7 => 1, 8 => 1, 9 => 1, 11 => 1, 12 => 1, 13 => 1, 14 => 1, 15 => 1, 17 => 1, 18 => 1, 19 => 1, 20 => 1, 22 => 1, 23 => 1, 24 => 1);
    private static $collection_data = array();
    private static $all_categoriesLevel1 = array();
    private static $all_categoriesLevel2 = array();
    private static $all_categoriesLevel3 = array();
    private static $all_categoriesLevel4 = array();
    private static $all_categoriesLevel5 = array();
    private static $all_categoriesLevel6 = array();

    /**
     * getDBConnection is a function that connect with DB
     * @return CDbConnection
     */
    protected static function getDbConnection() {
        // Returns the application components.
        $component = Yii::app()->getComponents(false);
        if (!isset($component['readonlydb']))
            return Yii::app()->db;
        if (!Yii::app()->readonlydb instanceof CDbConnection)
            return Yii::app()->db;
        return Yii::app()->readonlydb;
    }

    /**
     * lock_aquire is a function that prevent other commands run if any commands is running
     * @param <type> $t
     */
    private static function lock_aquire($t = null) {
        if ($t === null)
            $t = LOCK_EX | LOCK_NB;
        self::$lock_fn = dirname(__FILE__) . "/../runtime/search.lock";
        // Opens file.
        $fh = fopen(self::$lock_fn, 'w+');
        // Changes file mode
        @chmod(self::$lock_fn, 0777);
        if (!flock($fh, $t))
            return null;
        self::$lock = $fh;
        return $fh;
    }

    /**
     * lock_release is a function that remove the lock.
     * @return <type>
     */
    private static function lock_release() {
        if (!self::$lock_fn)
            return -1;
        // Portable advisory file locking
        flock(self::$lock, LOCK_UN);
        // Closes an open file pointer
        fclose(self::$lock);
        // Deletes a file
        @unlink(self::$lock_fn);
        return 0;
    }

    /**
     * Create file
     * @param <srting> $collection
     */
    private static function create_unavailable_file($collection) {
        $filename = Yii::app()->basePath . "/runtime/search_engine_down_$collection.txt";
        @touch($filename);
    }

    /**
     * Delete file
     * @param <string> $core
     */
    private static function delete_unavailable_file($collection) {
        $filename = Yii::app()->basePath . "/runtime/search_engine_down_$collection.txt";
        if (file_exists($filename)) {
            unlink($filename);
        }
    }

    private static function prepareSolrDocument($object, $limit = 1000) {
        $document = array();
        $document['id'] = (int) $object['id'];
        $document['title'] = $object['event'];

       
        Yii::app()->collection1->updateFieldsWithoutCommit($document);

        if (self::$dataSolr >= $limit) {
            self::$dataSolr = 0;
            
                Yii::app()->collection1->solrCommit();
                //Yii::app()->$coll->solrCommitWithOptimize();
            
            // Clear collection data
            self::$collection_data = array();
        } else {
            self::$dataSolr++;
        }
    }

    /**
     * Index all posts and it's info
     * @param <type> $mtime
     * @param <type> $city
     * @param <type> $limit
     * @param <type> $core
     */
    public static function fillData($mtime, $limit, $collection = 12, $actionType = null, $sleep = 0) {
       // var_dump(dirname(__FILE__).'/../../../Octopus/data/log.db');die();
        print "locking ... \n";
        // locking the command
        if ($actionType == "fullReindex") {
            if (!self::lock_aquire()) {
                die(" ** could not acuire lock!!\n");
            }
        }

        // Sets access and modification time of file , If the file does not exist, it will be created.
        if ($actionType == "fullReindex")
            self::create_unavailable_file($collection);

        $last_id = -1;
        // Get count of data that will be indexed .

        if ($mtime == -1) {
            $mtime = strtotime('2013-01-01 10:10:10');
            $time_db = -1;
        } else {
            $time_db = $mtime;
            $mtime = $mtime;
        }
        $sql = "select count(*) as count from log where time > '$mtime' ;";

        $count = self::getDbConnection()->createCommand($sql)->queryAll();
       
        // Print count of data.
        print "To be indexed (" . $count[0]["count"] . ") events" . chr(10);
        // initialize variable .
        $count = $count[0]["count"];
        $done_count = 0;
        $coll = "collection" . $collection;

        $size_of_data = 0;
        $done = false;
        $condition = "";
        

        while (!$done) {

            // Build Indexing Query


             $sql = "
				select *
                                from log
				where 
				time > '$mtime' and  
				id > $last_id
				order by id
				limit $limit
					";
             
            // execute the indexing query
            $events = self::getDbConnection()->createCommand($sql)->queryAll();
            // Get size of rows
            $size_of_data = sizeof($events);
            if ($size_of_data) {
                foreach ($events as $post) {
                   
                    self::prepareSolrDocument($post, $limit);
                    
                    $last_id = $post['id'];
                    if (isset($post['time']))
                        $time_db = $post['time'];
                }
            }// end foreach for Indexing process .

            $done_count+=$size_of_data;
            if ($count > 0) {
                printf("** done  with %d events, overall %g %%\n", $size_of_data, 100.0 * $done_count / $count);
            } else {
                print "No update events \n";
            }
            $done = (sizeof($events) < $limit);
        } // end while of objects
        // execute sleep .
        sleep($sleep);

        if ($size_of_data && $actionType != "update") {
            
                $coll = "collection1";
                Yii::app()->$coll->solrCommitWithOptimize();
            
        }

        if ($size_of_data && $actionType == "update") {

               
                    $coll = "collection1";
                    Yii::app()->$coll->solrCommit();

        }


        print "\n\n";

        $time = time();
        if ($time_db) {
            $time = $time_db;
        }
        $filename = Yii::app()->basePath . "/runtime/search_" . $collection . ".txt";
        touch($filename);
        file_put_contents($filename, $time);
        if ($collection = -1) {
            foreach (self::$countries_ids as $country => $i) {
                $filename = Yii::app()->basePath . "/runtime/search_" . $country . ".txt";
                touch($filename);
                file_put_contents($filename, $time);
            }
        }



        // Delete the files.
        if ($actionType == "fullReindex") {
            self::lock_release();
            self::delete_unavailable_file($collection);
            $filename = Yii::app()->basePath . "/runtime/search_cleanup" . $collection . ".txt";
            @touch($filename);
            @file_put_contents($filename, $time);
        }
        Print "\nDone \n";
    }

    /** This can be useful when (backwards compatible) changes have been made to  solrconfig.xml or schema.xml files
     * @param <int> $collection = 1 , 2 , 3
     */
    public static function actionReload($collection = 12) {
        if ($collection == -1) {
            foreach (self::$countries_ids as $country => $i) {
                print "Collection $country \n";
                $collection = "collection" . $country;
                shell_exec("wget -O - 'http://localhost:8983/solr/admin/cores?action=RELOAD&core=" . $collection . "'   1>/dev/null 2>/dev/null");
            }
        } else {
            try {
                if (!is_numeric($collection)) {
                    shell_exec("wget -O - 'http://localhost:8983/solr/admin/cores?action=RELOAD&core=" . $collection . "'   1>/dev/null 2>/dev/null");
                } else {
                    shell_exec("wget -O - 'http://localhost:8983/solr/admin/cores?action=RELOAD&core=collection" . $collection . "'   1>/dev/null 2>/dev/null");
                }

                print "\ndone\n";
            } catch (Exception $e) {
                print "Error : " . $e->getMessage();
            }
        }
    }

    /**
     * actionUpdate indexing the new data
     * @author Ramzi Sh. Alqrainy
     * @param <int> $limit
     * @param <int> $core 1 -> primary solr , 2 -> beta solr , -1 -> all
     * @param <int> $sleep
     */
    public function actionUpdate($limit = 3000, $collection = 12, $sleep = 0) {
        $time = time();
        $mtime = 0;
        $filename = Yii::app()->basePath . "/runtime/search_" . $collection . ".txt";
        // Reads entire file into a string.
        $contents = file_get_contents($filename);
        if ($contents == false) {
            $mtime = -1;
        } else {
            $mtime = (int) ($contents);
        }
        self::fillData($mtime, $limit, $collection, 'update', $sleep);
    }

    /**
     * actionFullReindex rebuild schema after remove it and indexing the data
     * @author Ramzi Sh. Alqrainy
     * @param <type> $city
     * @param <type> $limit
     * @param <type> $collection
     */
    public function actionFullReindex($limit = 3000, $collection = 12, $sleep = 0) {
        $q = "*:*";
        if ($collection == -1) {
            foreach (self::$countries_ids as $country => $i) {
                $coll = "collection" . $country;
                Yii::app()->$coll->rm($q);
            }
            self::fillData(-1, $limit, $collection, 'fullReindex', $sleep);
        } else {
            $coll = "collection" . $collection;
            Yii::app()->$coll->rm($q);
            self::fillData(-1, $limit, $collection, 'fullReindex', $sleep);
        }
    }

    /**
     * actionReindex re indexing the data
     * @param <int> $limit
     * @param <int> $collection
     * @param <int> $sleep=0
     */
    public function actionReindex($limit = 3000, $collection = 12, $sleep = 0) {
        self::fillData(-1, $limit, $collection, 'reindex', $sleep);
    }

    public function actionReindexPostByID($collection = 12, $post_id = 0) {
        self::deletePostByID($collection, $post_id);



        // Get information
        self::getCitiesInfo();
        self::getCountriesInfo();

        self::getCategoriesLevel1Info($collection);
        self::getCategoriesLevel2Info($collection);
        self::getCategoriesLevel3Info($collection);
        self::getCategoriesLevel4Info($collection);
        self::getCategoriesLevel5Info($collection);
        self::getCategoriesLevel6Info($collection);

        $sql = "select count(*) as count
				from posts as post
				where post.countries_id=$collection
				and  post.active=1 
				and  record_expiration_date> now() 
				and  deleted_by_member=0 ;";

        $count[0]["count"] = 100000;
        //$count = self::getDbConnection()->createCommand($sql)->queryAll();
        // Print count of data.
        print "To be indexed (" . $count[0]["count"] . ") posts" . chr(10);
        // initialize variable .
        $count = $count[0]["count"];
        $done_count = 0;
        $coll = "collection" . $collection;

        $size_of_data = 0;
        $done = false;
        while (!$done) {

            // Build Indexing Query


            $sql = "
				select post.id as id,
				post.location as location,
				post.title as title,
				post.description as  description,
				post.record_posted_date as record_posted_date ,
				subcategories_id as subcategories_id,
				post.new_cat_id as category_id,
				post.cities_id as cities_id,
				post.countries_id as countries_id,
				member.M_user_name as members_M_user_name,
				post.price as price,
				(select name from posts_images where posts_id=post.id limit 1 offset 0 ) as featured_image_name ,
				members_id,
				post.mv_ts as mv_ts,
				post.active as active,
				deleted_by_member
				from posts as post 
				left join members member on member.id = post.members_id
				where 
				post.countries_id=$collection and 
				record_expiration_date> now() and 
				post.id = $post_id
				order by post.id
					";

            // execute the indexing query
            $posts = self::getDbConnection()->createCommand($sql)->queryAll();
            // Get size of rows

            $size_of_data = sizeof($posts);
            if ($size_of_data) {
                foreach ($posts as $post) {
                    self::prepareSolrDocument($post, 1);
                }
            }// end foreach for Indexing process .

            $done_count+=$size_of_data;
            if ($count > 0) {
                printf("** done  with %d posts, overall %g %%\n", $size_of_data, 100.0 * $done_count / $count);
            } else {
                print "No update posts \n";
            }
            $done = (sizeof($posts) < 2);
        } // end while of objects

        Yii::app()->$coll->solrCommit();

        Print "\nDone \n";
    }

    /**
     * actionactionDeletePostByID action delete post with specific id from solr
     * @param <int> $collection
     */
    public static function actionDeletePostByID($collection = 0, $post_id = 0) {
        self::deletePostByID($collection, $post_id);
    }

    /**
     *
     */
    public static function deletePostByID($country_id, $post_id = 0) {
        print "delete post # $post_id of collection $country_id ... \n";
        $q = "id:$post_id";
        $coll = $country_id;
        if(is_numeric($country_id))$coll = "collection" . $country_id;
        Yii::app()->$coll->rm($q);

        print "\r[Done]\n";
    }

    /**
     * actionIndexSpamWords  indexing spam words from settings
     */
    public function actionIndexSpamWords() {
        Yii::app()->anti_spam->rm("*:*");
        $sql = "SELECT S_spam_words,S_comment_spam_words FROM settings;";
        print "Indexing ... \n";
        // execute the query
        $spam_words = self::getDbConnection()->createCommand($sql)->queryAll();
        // POST
        $document = array();
        $document['id'] = 1;
        $document['type'] = "post";
        $posts_spam_words = explode("\n", str_replace("\r", "", $spam_words[0]['S_spam_words']));
        $comments_spam_words = explode("\n", str_replace("\r", "", $spam_words[0]['S_comment_spam_words']));
        $count = 1;
        foreach ($posts_spam_words as $word) {
            if (!empty($word)) {
                $document = array();
                $document['id'] = $count;
                $document['type'] = "post";
                if (strpos($word, "@")) {
                    $document['text_st'] = $word;
                } else {
                    $document['text'] = $word;
                }
                Yii::app()->anti_spam->updateOneWithoutCommit($document);
                $count++;
            }
        }
        foreach ($comments_spam_words as $word) {
            if (!empty($word)) {
                $document = array();
                $document['id'] = $count;
                $document['type'] = "comment";
                if (strpos($word, "@")) {
                    $document['text_st'] = $word;
                } else {
                    $document['text_st'] = $word;
                    $document['text'] = $word;
                }
                Yii::app()->anti_spam->updateOneWithoutCommit($document);
                $count++;
            }
        }
        Yii::app()->anti_spam->solrCommitWithOptimize($document);
        print "Done\n";
    }

    /**
     * actionCleanup action remove archived and deleted post from solr
     * @param <int> $collection
     */
    public function actionCleanup($collection = 0) {
        $coll = "collection" . $collection;
        $time = time();
        $mtime = 0;
        $filename = Yii::app()->basePath . "/runtime/search_cleanup" . $collection . ".txt";
        // Reads entire file into a string.
        $contents = @file_get_contents($filename);
        if ($contents == false) {
            $mtime = -1;
        } else {
            $mtime = (int) ($contents);
        }
        print "delete unneeded data of collection $collection ... \n";
        // getting all ids from db ...
        print "getting all ids from db ...\n";
        $limit = 8000;
        $last_id = -1;
        $done = false;
        $ids = array();
        $last_time_db = -1;
        while (!$done) {
            $con = "id>" . $last_id . " and countries_id=$collection and mv_ts > $mtime limit $limit";
            $sql = "select id , mv_ts from posts_archive where " . $con;
            $posts = self::getDbConnection()->createCommand($sql)->query();
            foreach ($posts as $post) {
                Yii::app()->$coll->rmWithoutCommit($post["id"]);
                $ids[] = $post["id"];
                $last_id = $post["id"];
                if ($post["mv_ts"])
                    $last_time_db = strtotime($post['mv_ts']);
            }
            Yii::app()->$coll->solrCommitWithOptimize();
            print "Done with " . sizeof($posts) . "\n";
            $done = sizeof($posts) < $limit;
        }

        if (count($ids) > 0) {

            print "[Done]\n";
        } else {
            print 'No Data found in solr';
        }
        $time = time();
        if ($last_time_db)
            $time = $last_time_db;
        $filename = Yii::app()->basePath . "/runtime/search_cleanup" . $collection . ".txt";
        @file_put_contents($filename, time());
    }

    /**
     * actionClear action clears all data from solr
     * @param <int> $collection
     */
    public function actionClearPosts($collection = 0) {
        print "clearing data of collection $collection ... \n";
        $q = "*:*";
        $coll = "collection" . $collection;
        Yii::app()->$coll->rm($q);

        print "\r[Done]\n";
    }

    /**
     * actionIndex to show the info. search commands.
     * @author Ramzi Sh. Alqrainy
     */
    public function actionIndex() {
        echo "
--limit = Number \n\t use it if you want to increase/decrease the amount of data that will \n\t be withdrawn from the Database and stored in Solr at each loop until \n\t the end of all data , this is optional by default limit=3000\n
--collection = Number \n\t use it if you want to perform method on specfic solr component \n\t  , this is optional by default \n\t performs the method on jordan shard solr.\n ";
        echo $this->getHelp();
    }

    /**
     * Represents a response to a ping request to the server
     * @param <int> $collection
     */
    public function actionPing($collection = 12) {
        if ($collection == -1) {
            foreach (self::$countries_ids as $country => $i) {
                print "Collection $country \n";
                $coll = "collection" . $country;
                var_dump(Yii::app()->$coll->_solr->optimize());
            }
        } else {
            print "Collection $collection \n";
            $coll = "collection" . $collection;
            var_dump(Yii::app()->$coll->_solr->ping());
        }
    }

    /**
     * Defragments the index
     * @param <int> $collection
     */
    public function actionOptimize($collection = 12) {
        if ($collection == -1) {
            foreach (self::$countries_ids as $country => $i) {
                print "Collection $country \n";
                $coll = "collection" . $country;
                var_dump(Yii::app()->$coll->_solr->optimize());
            }
        } else {
            print "Collection $collection \n";
            $coll = "collection" . $collection;
            var_dump(Yii::app()->$coll->_solr->optimize());
        }
    }

    /**
     *
     * get keywords info from DB
     */
    public function actionIndexKeywords($limit = 20000) {
        Yii::app()->keywords->rm("*:*");

        $sql = "select count(*) as count FROM keyword_tag where indexable=1 and display_on_post_view = 1;";


        $count = self::getDbConnection()->createCommand($sql)->queryAll();
        // Print count of data.
        print "To be indexed (" . $count[0]["count"] . ") posts" . chr(10);
        // initialize variable .
        $count = $count[0]["count"];
        $last_id = -1;
        $done = false;
        $done_count = 0;
        $id = 1;
        while (!$done) {

            $sql = "select original.id , original.keyword,original.tag as tag, original.country_id as country,original.city_id as city, original.new_cat_id  as new_cat_id, original.synonyms as synonyms , kt.keyword as parent  FROM keyword_tag as original left join keyword_tag as kt  on original.parent_id=kt.id where original.id > $last_id and original.indexable=1 and display_on_post_view = 1 order by original.id  limit $limit";

            $keywords = self::getDbConnection()->createCommand($sql)->queryAll();


            foreach ($keywords as $keyword) {
                $document = array();
                $document['id'] = $keyword['id'];
                if ($keyword['parent'] == null) {
                    $document['type'] = 'keyword';
                } else {
                    $document['parent'] = $keyword['parent'];
                    $document['type'] = 'sub keyword';
                }
                $document['new_cat_id'] = $keyword['new_cat_id'];
                $document['keyword'] = $keyword['keyword'];
                $document['tag'] = $keyword['tag'];
                $document['country_id'] = $keyword['country'];
                $document['city_id'] = $keyword['city'];
                $synonyms = array();
                //$synonyms = axplode("،", $keyword['synonyms']);
                $synonyms = explode(",", $keyword['synonyms']);
                foreach ($synonyms as $synonym) {
                    $document['tags'][] = trim($synonym);
                }
                $document['tags'][] = $keyword['keyword'];
                $id++;
                Yii::app()->keywords->updateOneWithoutCommit($document);
                $last_id = $keyword['id'];
            }

            Yii::app()->keywords->solrCommitWithOptimize();
            $done_count+=sizeof($keywords);
            if ($count > 0) {
                printf("** done  with %d posts, overall %g %%\n", sizeof($keywords), 100.0 * $done_count / $count);
            } else {
                print "No update posts \n";
            }
            $done = sizeof($keywords) < $limit;
        }
    }

    /**
     * actionShowReport shows the report about how many projects in DB with solr
     * @author Ramzi Sh. Alqrainy
     * @param <int> $collection
     */
    public function actionShowReport($collection = 12) {
        self::getCountriesInfo();
        $q = "*:*";
        // when we need to show the report about solr
        if (isset(self::$all_countries[$collection])) {
            $country = self::$all_countries[$collection]['country_name'];
        } else {
            print "Collection $collection does not found";
            return;
        }
        print "\n ################### Solr Collection $collection Report ########################## \n \n";
        print "        Type   \t\t\t\tDatabase\t\tindex\n";

        $sql = "select count(*) as count
				from posts as post
				where post.countries_id=$collection
				and  post.active=1 
				and  record_expiration_date> now() 
				and  deleted_by_member=0 ;";

        $coll = "collection" . $collection;

        $results = Yii::app()->$coll->get($q, 0, 1, array());

        $project_count_sql = self::getDbConnection()->createCommand($sql)->queryRow();
        $project_count_sql = $project_count_sql['count'];
        print "\tPosts \t\t\t         " . $project_count_sql . "\t\t\t" . $results->response->numFound . " \n";


        print "\n \n";
    }

    /**
     *
     * get countries from DB
     */
    private static function getCountriesInfo() {
        $sql = "SELECT
				id as country_id,
				name as country_name,
				currency,
				price_format_character
				FROM countries ";
        $counries = self::getDbConnection()->createCommand($sql)->queryAll();
        foreach ($counries as $country) {
            self::$all_countries[$country['country_id']]['country_id'] = $country['country_id'];
            self::$all_countries[$country['country_id']]['country_name'] = $country['country_name'];
            self::$all_countries[$country['country_id']]['currency'] = $country['currency'];
            if (isset($country['price_format_character'])) {
                self::$all_countries[$country['country_id']]['price_format_character'] = $country['price_format_character'];
            } else {
                self::$all_countries[$country['country_id']]['price_format_character'] = " ";
            }
        }
    }

    /**
     *
     * get cities from DB
     */
    private static function getCitiesInfo() {
        $sql = "SELECT
				id as city_id,
				name as city_name
				FROM cities ";
        $cities = self::getDbConnection()->createCommand($sql)->queryAll();
        foreach ($cities as $city) {
            self::$all_cities[$city['city_id']]['city_id'] = $city['city_id'];
            self::$all_cities[$city['city_id']]['city_name'] = $city['city_name'];
        }
    }

    private static function getCategoriesLevel1Info($collection) {
        self::$all_categoriesLevel1 = array();
        //is_delete=0 and
        $sql = "select id,name,level,parent_id from categories_new where level=1 and countries_id=$collection";
        if ($collection == -1)
            $sql = "select id,name,level,parent_id from categories_new where level=1";
        $categories = self::getDbConnection()->createCommand($sql)->queryAll();
        foreach ($categories as $category) {
            self::$all_categoriesLevel1[$category['id']]['category_id'] = $category['id'];
            self::$all_categoriesLevel1[$category['id']]['category_name'] = $category['name'];
            self::$all_categoriesLevel1[$category['id']]['category_level'] = $category['level'];
            self::$all_categoriesLevel1[$category['id']]['parent_id'] = $category['parent_id'];
        }
    }

    private static function getCategoriesLevel2Info($collection) {
        self::$all_categoriesLevel2 = array();
        //cat1.is_delete=0 and cat1.active=1 and
        $sql = "select cat.id as id,cat.name as name,cat.level as level,cat.parent_id as parent_id from categories_new as cat left join categories_new as cat1 on cat.parent_id=cat1.id where  cat1.level=1 and cat1.countries_id=$collection";
        if ($collection == -1)
            $sql = "select id,name,level,parent_id from categories_new where is_delete=0 and level=2";
        $categories = self::getDbConnection()->createCommand($sql)->queryAll();
        foreach ($categories as $category) {
            self::$all_categoriesLevel2[$category['id']]['category_id'] = $category['id'];
            self::$all_categoriesLevel2[$category['id']]['category_name'] = $category['name'];
            self::$all_categoriesLevel2[$category['id']]['category_level'] = $category['level'];
            self::$all_categoriesLevel2[$category['id']]['parent_id'] = $category['parent_id'];
        }
    }

    private static function getCategoriesLevel3Info($collection) {
        $sql = "select id,name,parent_id,level,parent_id from categories_new where is_delete=0 and level=3 and countries_id=$collection";
        if ($collection == -1)
            $sql = "select id,name,level,parent_id from categories_new where is_delete=0 and level=3";
        $categories = self::getDbConnection()->createCommand($sql)->queryAll();
        foreach ($categories as $category) {
            self::$all_categoriesLevel3[$category['id']]['category_id'] = $category['id'];
            self::$all_categoriesLevel3[$category['id']]['category_name'] = $category['name'];
            self::$all_categoriesLevel3[$category['id']]['category_level'] = $category['level'];
            self::$all_categoriesLevel3[$category['id']]['parent_id'] = $category['parent_id'];
        }
    }

    private static function getCategoriesLevel4Info($collection) {
        $sql = "select id,name,parent_id,level,parent_id from categories_new where is_delete=0 and level=4 and countries_id=$collection";
        if ($collection == -1)
            $sql = "select id,name,level,parent_id from categories_new where is_delete=0 and level=4";
        $categories = self::getDbConnection()->createCommand($sql)->queryAll();
        foreach ($categories as $category) {
            self::$all_categoriesLevel4[$category['id']]['category_id'] = $category['id'];
            self::$all_categoriesLevel4[$category['id']]['category_name'] = $category['name'];
            self::$all_categoriesLevel4[$category['id']]['category_level'] = $category['level'];
            self::$all_categoriesLevel4[$category['id']]['parent_id'] = $category['parent_id'];
        }
    }

    private static function getCategoriesLevel5Info($collection) {
        $sql = "select id,name,parent_id,level,parent_id from categories_new where is_delete=0 and level=5 and countries_id=$collection";
        if ($collection == -1)
            $sql = "select id,name,level,parent_id from categories_new where is_delete=0 and level=5";
        $categories = self::getDbConnection()->createCommand($sql)->queryAll();
        foreach ($categories as $category) {
            self::$all_categoriesLevel5[$category['id']]['category_id'] = $category['id'];
            self::$all_categoriesLevel5[$category['id']]['category_name'] = $category['name'];
            self::$all_categoriesLevel5[$category['id']]['category_level'] = $category['level'];
            self::$all_categoriesLevel5[$category['id']]['parent_id'] = $category['parent_id'];
        }
    }

    private static function getCategoriesLevel6Info($collection) {
        $sql = "select id,name,parent_id,level,parent_id from categories_new where is_delete=0 and level=6 and countries_id=$collection";
        if ($collection == -1)
            $sql = "select id,name,level,parent_id from categories_new where is_delete=0 and level=6";
        $categories = self::getDbConnection()->createCommand($sql)->queryAll();
        foreach ($categories as $category) {
            self::$all_categoriesLevel6[$category['id']]['category_id'] = $category['id'];
            self::$all_categoriesLevel6[$category['id']]['category_name'] = $category['name'];
            self::$all_categoriesLevel6[$category['id']]['category_level'] = $category['level'];
            self::$all_categoriesLevel6[$category['id']]['parent_id'] = $category['parent_id'];
        }
    }

    /**
     * @param $keyword
     */
    public function actionReindexPostsByKeyword($collection = 12, $new_cat_id = null, $keyword = null, $sub_keyword = null) {
        $options = array(
            'facet' => 'true',
            'defType' => 'edismax',
            'facet.field' => array('keyword_name_str', 'sub_keyword_name_str'),
        );
        if ($keyword && $new_cat_id) {
            $options['fq'][] = "keyword_name_str:" . $new_cat_id . "-" . $keyword;
            if ($sub_keyword) {
                $options['fq'][] = "sub_keyword_name_str:" . $keyword . "-" . $sub_keyword;
            }
        }
        $coll = "collection" . $collection;
        $results = Yii::app()->$coll->get("*:*", 0, 20, $options);
        $document = array();
        if ($results->response->numFound > 20)
            $loop = ceil($results->response->numFound / 20);
        if ($results->response->numFound) {
            foreach ($results->response->docs as $doc) {

                $document['id'] = $doc->id;
                $document['title'] = $doc->title;
                $document['description'] = $doc->description;
                $document['location'] = $doc->location;
                $document['record_posted_time'] = $doc->record_posted_time;
                $document['record_posted_date'] = $doc->record_posted_date;
                $document['price'] = $doc->price;
                $document['member_name'] = $doc->member_name;
                $document['member_id'] = $doc->member_id;

                if (isset($doc->post_image_name))
                    $document['post_image_name'] = $doc->post_image_name;
                $document['category_id'] = (int) $doc->category_id;

                $document['tag_1_name_str'] = $doc->tag_1_name_str;
                $document['tag_1_name'] = $doc->tag_1_name;
                $document['tag_1_id'] = $doc->tag_1_id;
                if (isset($doc->tag_2_name_str)) {
                    $document['tag_2_name_str'] = $doc->tag_2_name_str;
                    $document['tag_2_name'] = $doc->tag_2_name;
                    $document['tag_2_id'] = $doc->tag_2_id;
                }

                //$document['highlight_start_date_name_str'] = $doc->highlight_start_date_name_str;
                //$document['highlight_end_date_name_str'] = $doc->highlight_end_date_name_str;
                $document['highlight_type_id'] = 0; //$doc->highlight_type_id;

                $document['country_id'] = (int) $doc->country_id;
                $document['currency'] = $doc->currency;
                $document['price_format_character'] = $doc->price_format_character;
                $document['country_name'] = $doc->country_name;
                $document['city_id'] = (int) $doc->city_id;
                $document['city_name'] = $doc->city_name;

                $keywords = Yii::app()->keywords->get($document['title'], 0, 10, $options);
                if ($keywords->response->numFound) {
                    foreach ($keywords->response->docs as $doc) {
                        if ($doc->keyword) {
                            if (isset($doc->tags))
                                $tags = get_object_vars($doc->tags);
                            $tags[] = $doc->keyword;
                            foreach ($tags as $tag) {
                                if (strpos($document['title'], $tag) !== false) {
                                    if ($doc->type == 'keyword') {
                                        $document['keyword_name_str'][] = trim($doc->new_cat_id . "-" . $doc->keyword);
                                    } else {
                                        $document['sub_keyword_name_str'][] = trim($doc->parent) . '-' . $doc->keyword;
                                    }
                                    break;
                                }
                            }
                        }
                    }
                }


                Yii::app()->keywords->rmWithCommit("id:" . $id);
                Yii::app()->keywords->updateOneWithoutCommit($document);
                $offset = $count + 20;
            }
            Yii::app()->keywords->solrCommitWithOptimize();
        }
    }

    /**
     * @param $id
     * @param $new_keyword
     */
    public function actionUpdateKeyword($id, $new_keyword) {
        $options = array(
            'facet' => 'true',
            'qf' => 'keyword tags',
            'q.op' => 'OR',
            'defType' => 'edismax',
            'facet.field' => array('id', 'country_id', 'new_cat_id'),
        );
        $options['fq'][] = "id:" . $id;
        $results = Yii::app()->keywords->get("*:*", 0, 10, $options);
        foreach ($results->response->docs as $doc) {
            echo "The keyword has this id $id is " . $doc->keyword . "\n";
            fwrite(STDOUT, "Are you sure ?(y/n)\n");
            // get input
            $answer = trim(fgets(STDIN));
            if ($answer == "y") {
                if ($results->response->numFound) {
                    foreach ($results->response->docs as $doc) {
                        $document = array();
                        $document['id'] = $id;
                        $document['type'] = $doc->type;
                        if (isset($doc->parent)) {
                            $document['parent'] = $doc->parent;
                        }
                        $document['new_cat_id'] = $doc->new_cat_id;
                        $document['keyword'] = $doc->keyword;
                        $document['country_id'] = $doc->country_id;
                        $document['city_id'] = $doc->city_id;
                        if (isset($doc->tags))
                            $document['tags'] = $doc->tags;
                        Yii::app()->keywords->updateOneWithoutCommit($document);
                        Yii::app()->keywords->solrCommitWithOptimize();
                    }
                }
            }
        }
    }

    public function actionDeleteKeywordById($id = 0) {
        fwrite(STDOUT, "Are you sure ?(y/n)\n");
        // get input
        $answer = trim(fgets(STDIN));
        if ($answer == "y") {
            Yii::app()->keywords->rm("id:" . $id);
            print "Done ;) \n";
        } else {
            print "bellah 3aleek rakkez :(";
        }
    }

    /**
     *
     * convert keyword corpora to sql
     */
    public function actionConvertKeywordCorporaToSQL() {
        fwrite(STDOUT, "Please insert the path that you want to create sql there.");
        // get input
        $path = trim(fgets(STDIN));
        $options = array(
            'facet' => 'true',
            'qf' => 'keyword tags',
            'q.op' => 'OR',
            'defType' => 'edismax',
            'facet.field' => array('id', 'country_id', 'new_cat_id'),
        );
        $options['fq'][] = "new_cat_id:70";
        $file = $path . '/keyword.txt';
        // Open the file to get existing content
        $current = file_get_contents($file);
        $current.="DROP TABLE  IF EXISTS car_tags; CREATE TABLE `car_tags` (
		  `id` int(11) NOT NULL AUTO_INCREMENT,
		  `keyword` varchar(255) NOT NULL,
		  `city_id` int(11) DEFAULT NULL,
		  `country_id` int(11) NOT NULL,
		  `parent_id` int(11) DEFAULT NULL,
		  `parent` varchar(2000) DEFAULT NULL,
		  `new_cat_id` int(10) unsigned DEFAULT NULL,
		  `synonyms` varchar(2000) DEFAULT NULL,
		  PRIMARY KEY (`id`),
		  KEY `keyword` (`keyword`),
		  KEY `country_id` (`country_id`,`city_id`),
		  KEY `new_cat_id` (`new_cat_id`)
		) ENGINE=InnoDB  CHARSET=utf8;";

        $results = Yii::app()->keywords->get("*:*", 0, 1000, $options);
        foreach ($results->response->docs as $doc) {
            $comma_separated = "";
            if (isset($doc->tags)) {
                $comma_separated = implode(",", $doc->tags);
            }

            // Append a new record to the file
            $current .= "
				INSERT INTO car_tags (keyword,city_id,country_id,parent_id,parent,new_cat_id,synonyms) VALUES ('" . $doc->keyword . "'," . $doc->city_id . "," . $doc->country_id . ",null," . (isset($doc->parent) ? "'" . $doc->parent . "'" : "null") . "," . $doc->new_cat_id . "," . "'" . $comma_separated . "');\n";
            // Write the contents back to the file
            file_put_contents($file, $current);
        }
    }

    /**
     *
     * Merge two keyword in Keyword Corpora
     */
    public function actionMergeKeywords($id1 = 0, $id2 = 0) {
        $options = array(
            'facet' => 'true',
            'qf' => 'keyword tags',
            'q.op' => 'OR',
            'defType' => 'edismax',
            'facet.field' => array('id', 'country_id', 'new_cat_id'),
        );
        $options['fq'][] = "id:" . $id2;
        $results = Yii::app()->keywords->get("*:*", 0, 10, $options);
        $options = array(
            'facet' => 'true',
            'qf' => 'keyword tags',
            'q.op' => 'OR',
            'defType' => 'edismax',
            'facet.field' => array('id', 'country_id', 'new_cat_id'),
        );
        $options['fq'][] = "id:" . $id1;
        $results2 = Yii::app()->keywords->get("*:*", 0, 10, $options);
        foreach ($results2->response->docs as $doc2) {
            echo $doc2->keyword . "\n";
        }
        foreach ($results->response->docs as $doc) {
            echo $doc->keyword . "\n";
        }
        fwrite(STDOUT, "Are you sure ?(y/n)\n");
        // get input
        $answer = trim(fgets(STDIN));
        if ($answer == "y") {
            if ($results->response->numFound) {
                foreach ($results->response->docs as $doc) {
                    $tags = array();
                    if (isset($doc->tags)) {
                        foreach ($doc->tags as $tag) {
                            $tags[] = $tag;
                        }
                    }
                    $tags[] = $doc->keyword;
                    foreach ($results2->response->docs as $doc2) {
                        if (isset($doc2->tags)) {

                            foreach ($doc2->tags as $tag) {
                                $tags[] = $tag;
                            }
                        }
                        $tags[] = $doc2->keyword;
                        $document = array();
                        $document['id'] = $id1;
                        $document['type'] = $doc2->type;
                        if (isset($doc2->parent)) {
                            $document['parent'] = $doc2->parent;
                        } else {
                            $document['parent'] = $doc2->parent;
                        }
                        $document['new_cat_id'] = $doc2->new_cat_id;
                        $document['keyword'] = $doc2->keyword;
                        $document['country_id'] = $doc2->country_id;
                        $document['city_id'] = $doc2->city_id;
                        $document['tags'] = $tags;
                        Yii::app()->keywords->rm("id:" . $id2);
                        Yii::app()->keywords->rm("id:" . $id1);
                        Yii::app()->keywords->updateOneWithoutCommit($document);
                        Yii::app()->keywords->solrCommitWithOptimize();
                    }
                }
            }
        }
    }
    
     public function actionIndexTerms($file = "",$collection=12) {
        Yii::app()->Terms->rm("country_id:".$collection);
        $csvFile = $file;
        $csv = self::readCSV($csvFile);
        $count = 0;
        $freq=800;
        $id=0;
        foreach ($csv as $term) {
            if($term[0]=="iphon" || $term[0]=="سكس" || $term[0]=="هون" || $term[0]=="بنات" || $term[0]=="sex" || $term[0]=="xnxx" || $term[0]=="xxnx" || $term[0]=="xxx")continue;
            $count++;
            if ($count < 8)
                continue;
            
            for($i = 1;$i<$freq;$i++){
                $document = array();
                $document['id'] = $id+($collection*30000);
                $document['term'] = $term[0];
                $document['country_id'] = $collection;
                Yii::app()->Terms->updateOneWithoutCommit($document);
                
                $id++;
            }
            if($count%3==0)$freq--;
            if ($count > 2400)
                break;
        }
        Yii::app()->Terms->solrCommitWithOptimize();
    }
    
    public function actionRemoveTerms($collection=12) {
        Yii::app()->Terms->rm("country_id:".$collection);
    }

    public static function readCSV($csvFile) {
        $file_handle = fopen($csvFile, 'r');
        while (!feof($file_handle)) {
            $line_of_text[] = fgetcsv($file_handle, 1024);
        }
        fclose($file_handle);
        return $line_of_text;
    }

    public function actionCategoriesMigration($collection = 2) {
        fwrite(STDOUT, "Please insert the path that you want to create sql there. \n");
        // get input
        $path = trim(fgets(STDIN));
        $file = $path . '/migration.txt';
        $rollback_file = $path . '/rollback_migration.txt';
        touch($file);
        touch($rollback_file);
        // Open the file to get existing content
        $current = file_get_contents($file);
        $old = file_get_contents($rollback_file);
        self::getCategoriesLevel1Info($collection);
        self::getCategoriesLevel2Info($collection);
        $time = time();
        $current = "SET autocommit = 0; START TRANSACTION;";
        $old = "SET autocommit = 0; START TRANSACTION;";
        $update = array(
            1261 => array(194, 206),
            1270 => array(192, 204),
            1278 => array(196),
            1280 => array(198),
            1288 => array(1248),
            1289 => array(200, 208),
            212 => array(212),
            214 => array(214),
            216 => array(216),
            218 => array(218),
            220 => array(220),
            222 => array(222),
            224 => array(224),
            226 => array(226),
            228 => array(228),
            1254 => array(194),
            1255 => array(206),
            232 => array(232),
            234 => array(234),
            236 => array(236),
            238 => array(238),
            240 => array(240),
            242 => array(242),
            244 => array(244),
        );
        foreach ($update as $k => $v) {
            // Build Solr query
            $options = array(
                'fl' => 'id,tag_1_id,tag_2_id',
                'qf' => 'title^1 description^1e-13 location^1e-13 tag_1_name^1 tag_2_name^1 city_name^1 price^1',
                'q.op' => 'AND',
                'bf' => "product(recip(sub($time,record_posted_time),1.27e-11,0.08,0.05),1000)^50",
                'defType' => 'edismax',
            );
            // Filter by subcategroy id
            if ($v) {
                $options["fq"][] = "{!tag=dt}tag_2_id:(" . implode(" OR ", $v) . ")";
                $options["facet"] = "true";
                $options["facet.field"][] = '{!ex=dt}tag_2_id';
            }
            $query = "*:*";
            $offset = 0;
            $limit = 1000;
            while (true) {
                $results = SearchLib::execute($query, $offset, $limit, $options, $collection);
                if (empty($results->response->docs))
                    break;
                foreach ($results->response->docs as $doc){
                    $new_cat_id = $k;
                    $old_cat_id = $doc->tag_2_id;
                    if ($new_cat_id)
                        $current .= "UPDATE posts SET new_cat_id=$new_cat_id WHERE id=$doc->id; \n";
                      if($old_cat_id)$old .= "UPDATE posts SET new_cat_id=$old_cat_id WHERE id=$doc->id; \n";
                }
                $offset = $offset + $limit;
            }
        }
        $cat = array(
            1256 => array(194, 206),
            1257 => array(194, 206),
            1258 => array(194, 206),
            1259 => array(194, 206),
            1260 => array(194, 206),
            
            1263 => array(192, 204),
            1264 => array(192, 204),
            1265 => array(192, 204),
            1266 => array(192, 204),
            1267 => array(192, 204),
            1268 => array(192, 204),
            1269 => array(192, 204),

            1272 => array(196),
            1273 => array(196),
            1274 => array(196),
            1275 => array(196),
            1276 => array(196),
            1277 => array(196),
//5adamat
                       1281 => array(222),
            1282 => array(222),
            1283 => array(222),
            1284 => array(222),
            1285 => array(222),
            1286 => array(222),
            1287 => array(222),
            1290 => array(242),
            1291 => array(242),
            1292 => array(242),
            1293 => array(242),
            1294 => array(242),
            1295 => array(242),
            1296 => array(242),
            1297 => array(242),
            1298 => array(242),
            1299 => array(242),
            1300 => array(242),
            1301 => array(242),
        );
        foreach (self::$all_categoriesLevel1 as $category) {
            foreach (self::$all_categoriesLevel2 as $subcategory) {
                if (!isset($cat[$subcategory['category_id']]))
                    continue;
                if ($subcategory['parent_id'] != $category['category_id'])
                    continue;

                // Build Solr query
                $options = array(
                    'fl' => 'id,tag_1_id,tag_2_id',
                    'qf' => 'title^1 description^1e-13 location^1e-13 tag_1_name^1 tag_2_name^1 city_name^1 price^1',
                    'q.op' => 'AND',
                    'bf' => "product(recip(sub($time,record_posted_time),1.27e-11,0.08,0.05),1000)^50",
                    'defType' => 'edismax',
                );
                // Filter by subcategroy id
                if ($cat[$subcategory['category_id']]) {
                    $options["fq"][] = "{!tag=dt}tag_2_id:(" . implode(" OR ", $cat[$subcategory['category_id']]) . ")";
                    $options["facet"] = "true";
                    $options["facet.field"][] = '{!ex=dt}tag_2_id';
                }
                $category_name = str_replace("معروضات للبيع", " ", $category['category_name']);
                $category_name = str_replace("سيارات ومركبات", "السيارات و المركبات", $category['category_name']);
                $category_name = str_replace("عقارات", "العقارات و الإسكان", $category['category_name']);
                $category_name = str_replace(" و", " ", $category['category_name']);
                $subcategory_name = str_replace(" و", " ", $subcategory['category_name']);
                $subcategory_name = str_replace("-", " OR ", $subcategory_name);
                $subcategory_name = str_replace("واكسسوارات", "", $subcategory_name);
                $subcategory_name = str_replace("اكسسوارات", "", $subcategory_name);
                $subcategory_name = str_replace("واكسسوراتها", "", $subcategory_name);
                $subcategory_name = str_replace("اكسسوراتها", "", $subcategory_name);
                $subcategory_name = str_replace("مستلزمات", "", $subcategory_name);
                $subcategory_name = str_replace("/", " OR ", $subcategory_name);
                $subcategory_name = str_replace("الطيور الحيوانات", "الطيور OR الحيوانات", $subcategory_name);
                $query = $subcategory_name;
                $offset = 0;
                $limit = 1000;

                while (true) {
                   
                    $results = SearchLib::execute($query, $offset, $limit, $options, $collection);
                    if (empty($results->response->docs))
                        break;
                    foreach ($results->response->docs as $doc) {
                        $new_cat_id = isset($subcategory['category_id']) ? $subcategory['category_id'] : $category['category_id'];
                         $old_cat_id = isset($doc->tag_2_id) ? $doc->tag_2_id : $doc->tag_1_id;
                        if ($new_cat_id)
                            $current .= "UPDATE posts SET new_cat_id=$new_cat_id WHERE id=$doc->id; \n";
                        if ($old_cat_id)
                          $old .= "UPDATE posts SET new_cat_id=$old_cat_id WHERE id=$doc->id; \n";
                    }
                    $offset = $offset + $limit;
                }
            }
        }
        $current.="commit;";
        $old.="commit;";
        file_put_contents($file, $current);
        file_put_contents($rollback_file, $old);
    }
    
    
    
    public function actionPostsMigration($collection = 2) {
        fwrite(STDOUT, "Please insert the path that you want to create sql there. \n");
        // get input
        $path = trim(fgets(STDIN));
        $file = $path . '/posts_migration.sql';
        touch($file);
        // Open the file to get existing content
        $current = file_get_contents($file);
        $done = false;
        $id = -1;
        while (!$done) {
            $sql = "select id,new_cat_id from posts where countries_id=$collection and id > $id order by id asc limit 15000 ;";
            // execute the indexing query
            $posts = self::getDbConnection()->createCommand($sql)->queryAll();
            $count = sizeof($posts);
            if ($count) {
                $current = "SET autocommit = 0; START TRANSACTION;";
                foreach ($posts as $post) {
                    $current .= "UPDATE posts SET new_cat_id=" . $post['new_cat_id'] . " WHERE id=" . $post['id'] . "; \n";
                    $id = $post['id'];
                }
            }
            $current.="commit;";
            file_put_contents($file, $current,FILE_APPEND);
            $posts = array();
            $current = "";
        }

        file_put_contents($file, $current,FILE_APPEND);
    }

}
