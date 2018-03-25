CREATE OR REPLACE FUNCTION cosine(l float[], r float[]) RETURNS float AS $$
DECLARE
  s float;
BEGIN
  s := 0;
  FOR i IN 1..array_length(l, 1) LOOP
    s := s + (l[i] * r[i]);
  END LOOP;
  RETURN |/ s;
END;
$$ LANGUAGE plpgsql;

