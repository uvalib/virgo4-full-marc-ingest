package main

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/applicationautoscaling"
	"github.com/aws/aws-sdk-go/service/ecs"
	"log"
)

func ensureServicesExist(services []string) error {
	return nil
}

// taken from https://docs.aws.amazon.com/sdk-for-go/api/service/ecs/#ECS.UpdateService

func serviceStop(clusterName string, serviceName string) error {

	sess := session.New()
	ecs_service := ecs.New(sess)
	aas_service := applicationautoscaling.New(sess)

	// suspend the autoscale rule application
	suspend := true
	suspend_state := &applicationautoscaling.SuspendedState{
		DynamicScalingInSuspended:  &suspend,
		DynamicScalingOutSuspended: &suspend,
		ScheduledScalingSuspended:  &suspend,
	}

	aas_params := &applicationautoscaling.RegisterScalableTargetInput{
		ResourceId:        aws.String(fmt.Sprintf("service/%s/%s", clusterName, serviceName)),
		ScalableDimension: aws.String("ecs:service:DesiredCount"),
		ServiceNamespace:  aws.String("ecs"),
		SuspendedState:    suspend_state,
	}

	// update autoscale rules
	_, err := aas_service.RegisterScalableTarget(aas_params)
	if err != nil {
		log.Println("Autoscale adjust failed, probably no autoscale rules")
	}

	// desired count to 0
	ecs_params := &ecs.UpdateServiceInput{
		DesiredCount: aws.Int64(0),
		Service:      aws.String(serviceName),
		Cluster:      aws.String(clusterName),
	}

	// update the service attributes
	_, err = ecs_service.UpdateService(ecs_params)
	if err != nil {
		return err
	}

	return nil
}

func serviceStart(clusterName string, serviceName string) error {

	sess := session.New()
	ecs_service := ecs.New(sess)
	aas_service := applicationautoscaling.New(sess)

	// un-suspend the autoscale rule application
	suspend := false
	suspend_state := &applicationautoscaling.SuspendedState{
		DynamicScalingInSuspended:  &suspend,
		DynamicScalingOutSuspended: &suspend,
		ScheduledScalingSuspended:  &suspend,
	}

	aas_params := &applicationautoscaling.RegisterScalableTargetInput{
		ResourceId:        aws.String(fmt.Sprintf("service/%s/%s", clusterName, serviceName)),
		ScalableDimension: aws.String("ecs:service:DesiredCount"),
		ServiceNamespace:  aws.String("ecs"),
		SuspendedState:    suspend_state,
	}

	// update autoscale rules
	_, err := aas_service.RegisterScalableTarget(aas_params)
	if err != nil {
		log.Println("Autoscale adjust failed, probably no autoscale rules")
	}

	// desired count to 1
	ecs_params := &ecs.UpdateServiceInput{
		DesiredCount: aws.Int64(1),
		Service:      aws.String(serviceName),
		Cluster:      aws.String(clusterName),
	}

	// update the service attributes
	_, err = ecs_service.UpdateService(ecs_params)
	if err != nil {
		return err
	}

	return nil
}

//
// end of file
//
