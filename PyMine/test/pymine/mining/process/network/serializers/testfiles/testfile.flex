<?xml version="1.0" ?>
<cnet>
	<net id="Causal Net - test_cnet" type="http://www.processmining.org"/>
	<name>test_cnet</name>
	<node id="a" isInvisible="false">
		<name>a</name>
	</node>
	<node id="b" isInvisible="false">
		<name>b</name>
	</node>
	<node id="c" isInvisible="false">
		<name>c</name>
	</node>
	<node id="d" isInvisible="false">
		<name>d</name>
	</node>
	<node id="e" isInvisible="false">
		<name>e</name>
	</node>
	<node id="f" isInvisible="false">
		<name>f</name>
	</node>
	<startTaskNode id="a"/>
	<endTaskNode id="f"/>
	<outputNode id="a">
		<outputSet>
			<node id="d"/>
			<node id="b"/>
			<node id="c"/>
		</outputSet>
		<outputSet>
			<node id="c"/>
			<node id="e"/>
			<node id="b"/>
		</outputSet>
	</outputNode>
	<outputNode id="b">
		<outputSet>
			<node id="f"/>
		</outputSet>
	</outputNode>
	<inputNode id="b">
		<inputSet>
			<node id="a"/>
		</inputSet>
	</inputNode>
	<outputNode id="c">
		<outputSet>
			<node id="f"/>
		</outputSet>
	</outputNode>
	<inputNode id="c">
		<inputSet>
			<node id="a"/>
		</inputSet>
	</inputNode>
	<outputNode id="d">
		<outputSet>
			<node id="f"/>
		</outputSet>
	</outputNode>
	<inputNode id="d">
		<inputSet>
			<node id="a"/>
		</inputSet>
	</inputNode>
	<outputNode id="e">
		<outputSet>
			<node id="f"/>
		</outputSet>
	</outputNode>
	<inputNode id="e">
		<inputSet>
			<node id="a"/>
		</inputSet>
	</inputNode>
	<inputNode id="f">
		<inputSet>
			<node id="c"/>
			<node id="e"/>
			<node id="b"/>
		</inputSet>
		<inputSet>
			<node id="d"/>
			<node id="b"/>
			<node id="c"/>
		</inputSet>
	</inputNode>
	<arc id="b5288b9a-8f58-4190-9ac2-be101bc47d7d" source="a" target="d"/>
	<arc id="c20be5e2-0c8b-4ed7-a639-63770da04832" source="a" target="b"/>
	<arc id="92fd87f8-934c-4ad0-8b68-f555a5089c52" source="a" target="c"/>
	<arc id="79685c9d-2ef1-43c6-a08e-4a6d59792b9c" source="a" target="e"/>
	<arc id="caff2ac8-d4db-4d96-9e1f-62df96dfcc1f" source="c" target="f"/>
	<arc id="ae7cca32-f2e4-44a2-a8d3-d50c02ee3a66" source="b" target="f"/>
	<arc id="0e3f4a09-e5c0-4018-9f35-a4149c50af16" source="d" target="f"/>
	<arc id="b719fcee-9779-4d35-baed-303e6d64e54c" source="e" target="f"/>
</cnet>
