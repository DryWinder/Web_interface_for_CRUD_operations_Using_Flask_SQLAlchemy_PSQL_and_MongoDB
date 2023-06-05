INSERT INTO tests_results (UML_test_status, UML_test_ball100, UML_test_ball12, UML_test_ball,
                           Ukr_test_status, Ukr_test_ball100, Ukr_test_ball12, Ukr_test_ball,
                           Hist_test_status, Hist_test_ball100, Hist_test_ball12, Hist_test_ball,
                           Math_test_status, Math_test_ball100, Math_test_ball12, Math_test_ball,
                           Phys_test_status, Phys_test_ball100, Phys_test_ball12, Phys_test_ball,
                           Chem_test_status, Chem_test_ball100, Chem_test_ball12, Chem_test_ball,
                           Bio_test_status, Bio_test_ball100, Bio_test_ball12, Bio_test_ball,
                           Geo_test_status, Geo_test_ball100, Geo_test_ball12, Geo_test_ball,
                           Eng_test_status, Eng_test_ball100, Eng_test_ball12, Eng_test_ball,
                           Fr_test_status, Fr_test_ball100, Fr_test_ball12, Fr_test_ball,
                           Deu_test_status, Deu_test_ball100, Deu_test_ball12, Deu_test_ball,
                           Sp_test_status, Sp_test_ball100, Sp_test_ball12, Sp_test_ball,
                           student_id)
SELECT UMLTestStatus, UMLBall100, UMLBall12, UMLBall,
       UkrTestStatus, UkrBall100, UkrBall12, UkrBall,
       HistTestStatus, HistBall100, HistBall12, HistBall,
       MathTestStatus, MathBall100, MathBall12, MathBall,
       PhysTestStatus, PhysBall100, PhysBall12, PhysBall,
       ChemTestStatus, ChemBall100, ChemBall12, ChemBall,
       BioTestStatus, BioBall100, BioBall12, BioBall,
       GeoTestStatus, GeoBall100, GeoBall12, GeoBall,
       EngTestStatus, EngBall100, EngBall12, EngBall,
       FrTestStatus, FrBall100, FrBall12, FrBall,
       DeuTestStatus, DeuBall100, DeuBall12, DeuBall,
       SpTestStatus, SpBall100, SpBall12, SpBall,
       s.student_id
FROM zno_records z
JOIN students s ON z.outid = s.outid
               AND z.birth = s.birth
               AND z.sextypename = s.sextypename;


ALTER TABLE students
ADD COLUMN tests_results_id INT NULL;

ALTER TABLE students
ADD FOREIGN KEY (tests_results_id) REFERENCES tests_results(tests_id);


UPDATE students
SET tests_results_id = tests_results.tests_id
FROM tests_results
WHERE students.student_id = tests_results.student_id;

