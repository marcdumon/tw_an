# --------------------------------------------------------------------------------------------------------
# 2020/06/17
# Twitter_Scraping - initial_users_TEMP.py
# md
# --------------------------------------------------------------------------------------------------------


WORDCLOUD_BLACKLIST = ['dag', 'mens', 'jaar', 'uur', 'tweet', 'video', 'politiekelent']

USERS_LIST = {
    'openvld': ['openvld', 'wfschiltz', 'BartSomers', 'ATurtelboom', 'DirkVanMechele1', 'ChristianLeysen', 'debackerphil', 'ElsAmpe', 'Dejonghe_Carla', 'svengatz', 'GuyVanhengel',
                'patriciaceysens', 'RikDaems', 'Maggie_DeBlock', 'IrinaDeKnop', 'Gwenny_De_Vroe', 'RuttenGwendolyn', 'dirksterckx', 'egbertlachaert', 'FilipAnthuenis',
                'MathiasDeClercq', 'alexanderdecroo', 'hermandecroo', 'JJDeGucht', 'martinetaelman', 'sasvanrouveroij', 'CarinaVanCauter', 'Barttommelein', 'MercedesVVolcem',
                'VincentVQ', 'PatrickDewael', 'KeulenMarino', 'StevenCoeneG', 'stephaniedhose', 'Gwenny_De_Vroe', 'SihameElk', 'tomongena', 'FreyaSaeys', 'emmilytalpe',
                'Mauritsvdr', 'rdecaluwe', 'TaniaDeJonge', 'DelvauxBram', 'katja_gabriels', 'goedeleliekens', 'TimVandenput', 'MarianeVerhaert', 'kverhelst'],

    'vlbelang': ['vlbelang', 'SamvanRooy1', 'WimVanOsselaer1', 'FDW_VB', 'gannemans', 'alexandracolen', 'JanPenris', 'Anke_online', 'DominiekLootens', 'Hagen_Goyvaerts',
                 'BartLaeremans', 'J_Van_Hauthem', 'Philip_Claeys', 'GuydhaeseleerVB', 'GuydhaeseleerVB', 'DVanLangenhove', 'A_Blancquaert', 'Filip_Bru', 'YvesBuysse',
                 'claesbart', 'johandeckmyn', 'ImmanuelReuse', 'chrisjanssensVB', 'Janlaeremans', 'IlseMalfroot', 'CarmenRyheul', 'kslagmulder', 'KlaasSlootmans', 'ElsSterckx',
                 'Barbara_Pas', 'KatleenBury', 'StevenCreyelman', 'OrtwinDepo', 'spiegeleer13', 'nathaliedewulf7', 'ErikGilissen', 'AnnickPonthier', 'KurtRavyts', 'DominiekSneppe',
                 'FrankTroosters', 'tomvangrieken', 'reccino', 'WouterVermeersc', 'HansVerreyt'],

    'de_NVA': ['de_NVA', 'Bart_DeWever', 'Zu_Demir', 'Vera_Celis', 'AnnickDeRidder', 'MinnekeDeRidder', 'DeWitSophie', 'marc_hendrickx', 'LiesbethHomans', 'JanJambon',
               'philippemuyters', 'KrisVanDijck1', 'reinildevm', 'TinevanderVloet', 'PietDeBruyn', 'markdemesmaeker', 'elsdemol', 'TineEerlingen', 'Willy_Segers', 'NadiaSminate',
               'kris10vanvaer', 'BenWeyts', 'FranckenTheo', 'sthbracke', 'peterdedecker', 'MDiependaele', 'ElkeSleurs', 'SarahSmeyers', 'StevensHelga', 'uyttersprotk',
               'mveetvelde', 'KVanOvermeire', 'MBeuselinck', 'ddumery', 'louis_ide', 'MaertensBert', 'StaesBart', 'WilfriedVdaele', 'GeertBourgeois', 'KaroGrosemans', 'lies_jans',
               'peerlux', 'svandeput', 'Veerle_Wouters', 'allessiaclaes', 'cathycoudyser', 'koendaniels', 'inez_deconinck', 'VreeseMaaike', 'gryffroy', 'SofieJoosen',
               'KathleenKrekels', 'joslantmeeters', 'MariusMeremans', 'Jnachtergaele', 'lorinparys123', 'FPerdaens', 'axel_ronse', 'StefaanSintobin', 'A_Tavernier', 'KarlVanlouwe',
               'VanMiertPaul', 'manuelavanwerde', 'KatjaVerheyen', 'bjanseeuw', 'PeterBuysrogge', 'KDepoorterMP', 'PeterDeRoover1', 'joydonne', 'MichaelFreilich', 'FriedaGijbels',
               'Yngvild8', 'SanderLoones', 'MetsuKoen', 'RaskinWouter', 'TRoggeman', 'SafaiDarya', 'SpoorenJan', 'anneleen_vb', 'YoleenVanCamp', 'vanderdoncktwim',
               'valerievanpeel', 'BertWollants', 'JorenVermeersc1'],

    'sp_a': ['sp_a', 'TMeeuws', 'kathleendeckx', 'carogennez', 'Turan_Guler', 'kvanbrempt', 'HanneloreGoeman', 'Fouad_Ahidar', 'bertanciaux', 'Yamila_Idrissi', 'SmetPascal',
             'elsedewachter', 'SaidElKhadraoui', 'conner_rousseau', 'kurtdeloor', 'MamaDaktari', 'BrunoTuybens', 'freyabos', 'dirkvdmaelen', 'bartvanmalderen', 'philippedecoene',
             'mhostekint', 'RLanduyt', 'johncrombez', 'MeryameKitir', 'ingridlieten', 'ChokriMahassine', 'elsrobeyns', 'LudoSannen', 'VvelthovenPeter', 'JorisVDBroucke',
             'HannesAnaf', 'LambrechtAnnick', 'katiasegers', 'LiseVDC', 'steve_vdb', 'LudwigVandenho1', 'maximveys', 'janbertels', 'MelissaDepr', 'KarinJiroflee', 'SegersBen',
             'AnjaVanrobaeys', 'kris_verduyckt'],

    'cdenv': ['cdenv', 'CDenV_VlaParl', 'Dirk_de_Kort', 'KathleenHelsen', 'ward_kennes', 'NahimaLanjri', 'peeters_kris1', 'TinneRombouts', 'KatrienSchryver', 'grietsmaers',
              'mariannethyssen', 'torfsrik', 'jefvandenbergh', 'ServaisV', 'Paul_Delva', 'BGrouwels', 'StevenVanackere', 'SammyMahdi', 'SonjaBecq', 'BrouwersKarin', 'Carldevlies',
              'jeanlucdeheane', 'tomdehaene', 'SchouppeEtienne', 'RobrechtB', 'pieterdecrem', 'leendierick', 'FranssenCindy', 'JokeSchauvliege', 'veliyuksel', 'joachimcoens',
              'hendrikbogaert', 'GrietCoppe', 'SabinedeBethune', 'StefaanDeClerck', 'martine_menen', 'YLeterme', 'nathaliemuylle', 'SabinePoleyn', 'JohanVerstreken', 'crevits',
              'wbeke', 'IvoBelet', 'LodeCeyssens', 'veerleheeren', 'verajans3620', 'SauwensJohan', 'Rafterwingen', 'JoVandeurzen', 'jobrouns1', 'RudderMaaike', 'BartDochy',
              'katparty', 'KVDHeuvel_VP', 'orry_vdw', 'LoesVandromme', 'vincent_v_p', 'Petervanrompuy', 'KurtVanryckegh1', 'bwarnez', 'JanBriersGouv', 'FrankyDemon',
              'stevenmathei', 'BercySlegers', 'ElsVanHoofcdenv'],

    'pvdabelgie': ['pvdabelgie', 'peter_mertens', 'JosDHaese', 'TomDMeester', 'DeWitteKim', 'ColebundersGaby', 'DaemsGreet'],

    'groen': ['groen', 'kristofcalvo', 'MeyremAlmaci', 'FreyaPiryns', 'MiekeVogels', 'elkevdbrandt', 'LuckasV1', 'Bex_Stijn', 'hermessanctorum', 'SVHecke', 'BartCaron',
              'geertlambert', 'BartStaes', 'WouterDeVriendt', 'ImadeAnnouri', 'JohanDanen', 'AMartelaer', 'CeliaLedoux', 'anmoerenhout', 'Elisameuleman', 'StafPelckmans',
              'BjornRzoska', 'miekeschauv', 'ChrisSteenwegen', 'JeremieVaneeckh', 'BuystKim', 'barbaracreemers', 'JessikaSoors', 'DieterVanBesien', 'EvitaWillaert'],

    'ldd': ['lijstdedecker', 'verstrepen', 'Boudewijnboucka', 'DeWaelePatricia', 'JMDedecker', 'IvanSabbe', 'UllaWerbrouck'],

    'kranten': ['t_pallieterke', 'HLN_BE', 'knack', 'gva', 'vrtnws', 'VTMNIEUWS', 'demorgen', 'tijd', 'destandaard', 'DoorbraakBe', 'SCEPTR_online', 'Apache_be', 'mondiaalnieuws',
                'dewereldmorgen', 'Newsmonkey_BE', 'terzaketv'] + ['Nieuwsblad_be', 'hbvl', 'humo'],

    'idioten': ['vvb_org', 'vlaams_leeuwtje', 'ockhams', 'LuytenCarina', 'IENouwen', 'PhRoose', 'volkinnood'],

    'rechts': ['IngPeeters', 'VBAntwerpen', 'VrouwenTegen', 'BartLaeremans', 'VlaanderenVL', 'reactnieuws', 'vbjongeren'],

    'opiniemakers':
        ['Smienos', 'GeertNoels', 'wduyck', 'Karl_Drabbe', 'DaveSinardet', 'MmiraSam', 'ReynebeauM', 'yvesd', 'mboudry', 'Stijn_Baert', 'IveMarx', 'FKeuleneer',
         'LinksVlaams', 'LudwigBollaerts', 'cauwelaert', 'alainmoutonVL', 'ArmandVervaeck', 'TBeeckman', 'mohamedouaamari', 'Youssef_Kobo',
         'DOBBELAEREW', 'KrisRemels', 'miadoo', 'LuckasV1', 'bouboutyassine', 'GuillaumeVdS', 'IvanVandeCloot'] +
        ['NoelSlangen', 'jdceulaer', 'mohamedouaamari', 'MelkMuylle', 'OrakelvMerksem', 'barteeckhout', 'JimmyKoppen'] +
        ['Pensioenspook', 'CharlesUrbain', 'devosmarc', 'PeterDeKeyzer', 'CarolineVen', 'PieterVBO', 'walterpauli'] +
        ['brinckie', 'NolfJan', 'jshln', 'biekepurnelle', 'Aya_Sabi', 'Ljosmyndun', 'hHetty', 'SaidBataray'] +
        ['lievenscheire', 'Poodle_Soup', 'NathaSupertramp', 'KoenVanderElst', 'andreastirez', 'kurtbeheydt', 'Zebbedeusje', 'Deopiniemaker', 'idevisch'],

    'vakbonden': ['vakbondABVV', 'ACV_Vakbond', 'BewegingNet', 'ACLVB', 'UNIZOvzw', 'btb_abvv', 'VBOFEB'],

    'denktanks': ['Liberalesbe', 'liberasvzw', 'ItineraTwit', 'Logia_vzw', 'DenktankMinerva', 'OikosDenktank'],

    'problems': []
}
