import sys
import yaml

with open(sys.argv[1], "r") as f:
	print("\n".join(["| %s | ```%s``` |" % (e["count"], e["fingerprint"].strip()) for e in yaml.safe_load(f)]))
