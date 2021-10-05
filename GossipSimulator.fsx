#r "nuget: Akka, 1.4.25"
#r "nuget: Akka.FSharp"
#r "nuget: Akka.Remote"

open System
open Akka.Actor
open Akka.FSharp

let clock = Diagnostics.Stopwatch()

type ActionType =
    | SetUp of IActorRef []*string*string
    // | IntializeAll of IActorRef []
    | Gossip of String
    | Start
    | RumorCount of String
    | PushSum
    | CalculatePS of Double * Double 
    | Terminate of String * Double * Double
    | SetMaxRumors of int

let nodeCount = int (fsi.CommandLineArgs.[1])
let topology = string (fsi.CommandLineArgs.GetValue 2)
let protocol = string (fsi.CommandLineArgs.GetValue 3)
let diff= 10.0 ** -10.0
let system = ActorSystem.Create("System")

let getRandArrElement =
  let rnd = Random()
  fun (arr : int[]) -> arr.[rnd.Next(arr.Length)]

type Tracker(nodes: IActorRef [], topology: string, protocol: string) =
    inherit Actor()

    override x.OnReceive(msg) =
        match msg :?> ActionType with
        | Start -> 
                for i in 1..nodes.Length-1 do
                    nodes.[i] <! SetUp(nodes,topology,protocol)
                let randNode = System.Random().Next(1, nodeCount+1)
                printfn "Start RadnodeVal %d" randNode
                if protocol="gossip" then
                    clock.Start()
                    nodes.[randNode]<! Gossip("I have a gossip")
                elif protocol="push-sum" then
                    clock.Start()
                    printfn "RandNode at start %d " randNode 
                    nodes.[randNode]<!PushSum
        | Terminate (proto,sum,weight)->
            //TODO:May have to send ratio
            clock.Stop()
            printfn "Time taken for convergence: %O" clock.Elapsed
            if proto="push-sum" then       
                printfn "For Push-sum:- Sum is %f, Weight is %f, and SumEstimate %.10f" sum weight (sum/weight)

            // Environment.Exit(0)
            //TODO:kill logic
        | _ ->()


type Node(neighbours: int [], nodeNum: int) =
    inherit Actor()
    let neighboursForMe=neighbours
    let mutable network : IActorRef [] =[||]
    let mutable topo=""
    let mutable proto=""
    let mutable tracker:IActorRef = null
    let mutable rumorCount=0
    let mutable sum:Double=nodeNum |> double
    let mutable weight:Double= 1.0
    let mutable epochCounter=0
    override x.OnReceive(msg) =
        match msg :?> ActionType with
        | SetUp (nodes: IActorRef [],topology:string,protocol:string)->
            // printfn "INside"
            network<-nodes
            topo<-topology
            proto<-protocol
            tracker<- x.Sender
            
        | Gossip rumor -> 
            // printfn "Creatint acotr"
            if rumorCount=10 then
                // printfn "Calling terminate %d" nodeNum
                tracker<! Terminate("gossip",0.,0.)
            else 
                let randNode = getRandArrElement neighboursForMe
                // printfn "val i %d and rumorCount %d" nodeNum rumorCount
                network.[randNode]<! Gossip(rumor)
                rumorCount<-rumorCount+1
        | PushSum  ->
            // let randNode = System.Random().Next(0, neighboursForMe.Length)
            let randNode = getRandArrElement neighboursForMe
            // printfn "val i %d " nodeNum 
            sum<-sum/2.0
            weight<-weight/2.0
            network.[randNode]<! CalculatePS(sum, weight)
        | CalculatePS (s:Double,w:Double)->
            let newSum=sum+s
            let newWeight=weight+w
            let ratioDiff=sum/weight-newSum/newWeight |> abs
            // printfn "val i %f " newWeight 
            printfn "val i %f " ratioDiff 

            if ratioDiff<diff then
                epochCounter<-epochCounter+1
            else 
                epochCounter<-0
            if epochCounter<3 then
                sum<-(sum+s)/2.0
                weight<-(weight+w)/2.0
                let index =getRandArrElement neighboursForMe
                network.[index]<! CalculatePS (sum,weight)
            else
                tracker<! Terminate ("push-sum",sum,weight)

        | _ -> ()



let rec getNeighbours (id: int) (topology: string) (nodeCount: int) : int [] =
    let mutable neighbourArray: int [] = [||]

    match topology with
        | "full" ->     
            for i in 1 ..nodeCount do
                if i<>id then
                    neighbourArray<-[|i|] |>Array.append neighbourArray
            
        | "line" -> 
                if id=1 then
                    neighbourArray<- [|id+1|] |>Array.append neighbourArray
                elif id=nodeCount then
                    neighbourArray<- [|id-1|] |>Array.append neighbourArray
                else
                    neighbourArray<- [|id+1|] |>Array.append neighbourArray
                    neighbourArray<- [|id-1|] |>Array.append neighbourArray
        | "3d" ->()
        | "imp3d" ->
                neighbourArray<- getNeighbours id "3d" nodeCount |> Array.append neighbourArray
                let mutable randNode = System.Random().Next(1, nodeCount+1)
                
                while Array.contains randNode neighbourArray || randNode <> id do
                    randNode <- System.Random().Next(1, nodeCount+1)
                
                neighbourArray<-[|randNode|]|> Array.append neighbourArray
        | _ ->printfn "Wrong Topology is provided as input, Please pass either of these- full, line, 3d, imp3d"
    neighbourArray

//--------program start
let nodeArrayOfActors = Array.zeroCreate (nodeCount+1)

let nodeList = [ 1 .. nodeCount]

for i in nodeList do
    let neighbours = getNeighbours i topology nodeCount// Make sure neighbours is iterated from 0
    nodeArrayOfActors.[i] <- system.ActorOf(Props.Create(typeof<Node>, neighbours, i), "demo" + string (i))
    //Make sure nodeArrayOfActors is iterated from 1
    

let trackerRef = system.ActorOf(Props.Create(typeof<Tracker>, nodeArrayOfActors,topology,protocol), "tracker")


trackerRef<! Start