package crdt

import (
	"potionDB/crdt/clocksi"
	"potionDB/crdt/proto"
)

// Add this import statement

type NoOpCrdt struct {
	CRDTVM
	//Adicionar outros campos
}

//Nota: no codigo (e.g., no counterCRDT) quando vires "Effect", nao confundas com o Effect nos no-ops.
//Aqui no PotionDB, effect significa qual o "efeito" que uma operação teve no estado do CRDT.
//Este "efeito" é usado pelos CRDTs pela gestão de versões, de modo a poder-se recalcular versões antigas dos objectos.

func (crdt *NoOpCrdt) GetCRDTType() proto.CRDTType { return proto.CRDTType_NOOP }

func (crdt *NoOpCrdt) Initialize(startTs *clocksi.Timestamp, replicaID int16) (newCrdt CRDT) {
	return &NoOpCrdt{
		CRDTVM: (&genericInversibleCRDT{}).initialize(startTs, crdt.undoEffect, crdt.reapplyOp, crdt.notifyRebuiltComplete),
		//Adicionar outros campos
	}
}

// Used to initialize when building a CRDT from a remote snapshot
func (crdt *NoOpCrdt) initializeFromSnapshot(startTs *clocksi.Timestamp, replicaID int16) (sameCRDT *NoOpCrdt) {
	crdt.CRDTVM = (&genericInversibleCRDT{}).initialize(startTs, crdt.undoEffect, crdt.reapplyOp, crdt.notifyRebuiltComplete)
	return crdt
}

func (crdt *NoOpCrdt) IsBigCRDT() bool { return false }

func (crdt *NoOpCrdt) Read(args ReadArguments, updsNotYetApplied []UpdateArguments) (state State) {
	//TODO. Deixa este método para depois do Update e Downstream. Ignora o updsNotYetApplied e o args.
	//O state deves ser tu proprio a definir, apenas precisa de implementar dois métodos:
	//GetCRDTType() proto.CRDTType:		{return proto.CRDTType_NOOP}
	//GetREADType() proto.READType: 	{return proto.READType_FULL}
	return nil
}

// Prepare
func (crdt *NoOpCrdt) Update(args UpdateArguments) (downstreamArgs DownstreamArguments) {
	//TODO: Este e o prepare. Faz aqui o codigo necessario para gerar os blocks e afins.
	//No final, deves retornar a operacao a ser executada na fase do effect.
	return nil
}

// Effect
func (crdt *NoOpCrdt) Downstream(updTs clocksi.Timestamp, downstreamArgs DownstreamArguments) (otherDownstreamArgs DownstreamArguments) {
	//TODO: Este é o effect. Regra geral aqui costumo chamar um metodo auxiliar "applyDownstream" que aplica mesmo os efeitos da operacao
	//Nesta fase como precisas do updTs, diria que podes fazer aqui a logica do effect (nomeadamente gerar os blocks)
	//A operacao em si pode ser aplicada na applyDownstream
	//No final podes retornar nil.
	//Por agora deixa a linha seguinte comentada: mais tarde será necessária.
	//crdt.addToHistory(&updTs, &downstreamArgs, effect)
	crdt.applyDownstream(downstreamArgs) //Podes alterar esta se precisares
	return nil
}

// Nota: este metodo nao faz parte da interface CRDT, por isso se precisares podes apaga-lo.
func (crdt *NoOpCrdt) applyDownstream(downstreamArgs DownstreamArguments) (effect *Effect) {
	//TODO: Metodo auxiliar do downstream.
	//Por agora em termos de retorno podes deixar o que pus aqui
	var effectV Effect = NoEffect{}
	return &effectV
}

func (crdt *NoOpCrdt) IsOperationWellTyped(args UpdateArguments) (ok bool, err error) {
	return true, nil
}

func (crdt *NoOpCrdt) Copy() (copyCRDT InversibleCRDT) {
	newCRDT := NoOpCrdt{
		CRDTVM: crdt.CRDTVM.copy(),
		//Adicionar outros campos que pertençam ao NoOpCrdt
	}
	return &newCRDT
}

func (crdt *NoOpCrdt) RebuildCRDTToVersion(targetTs clocksi.Timestamp) {
	//TODO: Might be worth it to make one specific for registers
	crdt.CRDTVM.rebuildCRDTToVersion(targetTs)
}

func (crdt *NoOpCrdt) reapplyOp(updArgs DownstreamArguments) (effect *Effect) {
	return crdt.applyDownstream(updArgs)
}

func (crdt *NoOpCrdt) undoEffect(effect *Effect) {
	//Do not fill this function for now.
}

func (crdt *NoOpCrdt) notifyRebuiltComplete(currTs *clocksi.Timestamp) {}

func (crdt *NoOpCrdt) GetCRDT() CRDT { return crdt }
