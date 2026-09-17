.PHONY: demo baseline fixed test clean logs

demo:
	./demo.sh

baseline:
	./scripts/run.sh baseline

fixed:
	./scripts/run.sh fixed

test:
	./scripts/run-all.sh

clean:
	docker compose --project-name pgstream-sequence-lab down --volumes --remove-orphans

logs:
	docker compose --project-name pgstream-sequence-lab logs --tail 200
